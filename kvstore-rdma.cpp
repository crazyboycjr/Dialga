/*
 * RDMA KVStore Implementation
 */
#include "kvstore-rdma.hpp"

#include <arpa/inet.h>
#include <gflags/gflags.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <strings.h>
#include <sys/socket.h>

#include <thread>

#include "config.hpp"
#include "kvstore.hpp"
#include "rdmatools.hpp"

namespace kvstore {
DEFINE_string(connect, "",
              "The connection info (ip:port,ip:port, ...). E.g., "
              "\"192.168.0.1:12000,192.168.0.2:12001\"");
DEFINE_int32(port, 12000, "The server listen port (TCP).");

std::vector<std::string> RdmaKVStore::GetHostList(const std::string& str) {
  std::vector<std::string> result;
  std::stringstream s_stream(str);
  while (s_stream.good()) {
    std::string substr;
    getline(s_stream, substr, ',');
    result.push_back(substr);
  }
  return result;
}

void RdmaKVStore::ParseHostPort(const std::string& str, std::string& host,
                                int& port) {
  std::stringstream s_stream(str);
  std::string port_str;
  // TODO: dirty hardcode here.
  getline(s_stream, host, ':');
  getline(s_stream, port_str);
  port = atoi(port_str.c_str());
}

int RdmaKVStore::Init() {
  manager_ = new RdmaManager(FLAGS_dev);
  if (manager_->InitDevice()) {
    LOG(ERROR) << "RdmaKVStore::InitDevice() failed";
    return -1;
  }
  // Client doesn't need to InitMemory()
  // if (manager_->InitMemory()) {
  //   LOG(ERROR) << "RdmaKVStore::InitMemory() failed";
  //   return -1;
  // }
  auto hostvec = GetHostList(FLAGS_connect);
  for (auto host : hostvec) {
    std::string hostname;
    int port;
    ParseHostPort(host, hostname, port);
    auto conn = manager_->Connect(hostname, port);
    if (!conn) {
      LOG(ERROR) << "RDMA Connect to " << hostname << " : " << port
                 << " failed";
      return -1;
    }
    connections_.push_back(conn);
    conn->SetIdx(connections_.size());
  }
  // Create a pooling thread to handle the poll;
  // There should be ONLY ONE polling thread.
  polling_thread_ = std::thread(&RdmaKVStore::Poll, this);
  polling_thread_.detach();
  LOG(INFO) << "Polling thread starts to busy-polling!";
  return 0;
}

void RdmaKVStore::Poll() {
  while (true) {
    for (auto conn : connections_) {
      auto cq =
          conn->GetQp()
              ->send_cq;  // NOTICE: we use the same cq for both send/recv.
      int n = 1;
      while (n > 0) {
        struct ibv_wc wc[kCqPollDepth];
        n = ibv_poll_cq(cq, kCqPollDepth, wc);
        if (n < 0) {
          PLOG(ERROR) << "ibv_poll_cq() failed";
          return;
        }
        for (int i = 0; i < n; i++) {
          LOG(INFO) << "Completion generated.";
          if (wc[i].status != IBV_WC_SUCCESS) {
            LOG(ERROR) << "Got bad completion status with " << wc[i].status;
            // TODO: maybe error handling here
            return;
          }
          auto wr_context = (WrContext*)(wc[i].wr_id);
          switch (wc[i].opcode) {
            case IBV_WC_RDMA_WRITE:
            case IBV_WC_RDMA_READ:
            // TODO: more logics for v2
            case IBV_WC_SEND:
              wr_context->ref_--;
              if (wr_context->ref_ == 0) {
                if (wr_context->cb_) wr_context->cb_();
                delete wr_context;
              }
              break;
            case IBV_WC_RECV:
              // TODO: PUT the recv buffer value to the user's designated
              // address
              break;
            default:
              LOG(ERROR) << "Other Ops are not supported. Must be error.";
              break;
          }
        }
      }
    }
  }
}

int RdmaKVStore::Get(const std::vector<Key>& keys,
                     const std::vector<Value*>& values, const Callback& cb) {
  return 0;
}

int RdmaKVStore::Put(const std::vector<Key>& keys,
                     const std::vector<Value>& values, const Callback& cb) {
  if (keys.size() != values.size()) {
    LOG(ERROR) << "Put() failed due to size mismatch (key & value)";
    return -1;
  }
  auto wr_context = new WrContext(0, cb);
  int to_post = keys.size();
  struct ibv_send_wr wr_list[kMaxConnection][kMaxBatch];
  struct ibv_send_wr* bad_wr;
  struct ibv_sge sgs[kMaxConnection][kMaxBatch];
  int wr_index[kMaxConnection];
  memset(wr_index, 0, sizeof(wr_index));
  memset(wr_list, 0, sizeof(wr_list));
  memset(sgs, 0, sizeof(sgs));
  int i = 0;
  while (i < to_post) {
    // Get the QP(connection) idx.
    auto key_index = indexs_.find(keys[i]);
    int conn_idx;
    if (key_index == indexs_.end()) {  // New key;
      conn_idx = i % connections_.size();
      IndexEntry newentry(conn_idx, 0, values[i].size_);
      indexs_.insert({keys[i], newentry});
    } else
      conn_idx = key_index->second.qp_index_;
    // Generate WR at right position
    int wr_position = wr_index[conn_idx];
    wr_index[conn_idx]++;
    char* buffer = (char*)values[i].addr_;
    TxMessage* msg_buffer = (TxMessage*)(buffer + values[i].size_);
    auto iter = memory_regions_.find(values[i].addr_);
    if (iter == memory_regions_.end()) {
      LOG(ERROR) << "Values are not registered.";
      return -1;
    }
    msg_buffer->key_ = keys[i];
    msg_buffer->opcode_ = KV_PUT;
    sgs[conn_idx][wr_position].addr = values[i].addr_;
    sgs[conn_idx][wr_position].length = values[i].size_ + sizeof(TxMessage);
    if (sgs[conn_idx][wr_position].length < iter->second->length) {
      LOG(ERROR) << "Value's length exceed registered buffer size";
      return -1;
    }
    // Message Format: [real value][control message]
    sgs[conn_idx][wr_position].lkey = iter->second->lkey;
    wr_list[conn_idx][wr_position].num_sge = 1;
    wr_list[conn_idx][wr_position].sg_list = &sgs[conn_idx][wr_position];
    wr_list[conn_idx][wr_position].opcode = IBV_WR_SEND;
    i++;
    if (wr_index[conn_idx] == kMaxBatch) {
      // This batch should be sent out
      wr_list[conn_idx][wr_position].next = nullptr;
      wr_list[conn_idx][wr_position].send_flags = IBV_SEND_SIGNALED;
      wr_list[conn_idx][wr_position].wr_id = (uint64_t)wr_context;
      wr_context->ref_++;
      if (ibv_post_send(connections_[conn_idx]->GetQp(), wr_list[conn_idx],
                        &bad_wr)) {
        PLOG(ERROR) << "ibv_post_send() for non-last batch failed.";
        return -1;
      }
      wr_index[conn_idx] = 0;
      memset(wr_list[conn_idx], 0, sizeof(wr_list[conn_idx]));
      // no need to memset sgs because sgs's value will always be filled.
    } else if (i != to_post) {
      wr_list[conn_idx][wr_position].next =
          &wr_list[conn_idx][wr_index[conn_idx]];
      wr_list[conn_idx][wr_position].send_flags = 0;
    }
  }
  // The last batch for each connection.
  for (size_t j = 0; j < connections_.size(); j++) {
    if (wr_index[j] > 0) {
      // there is to flush
      wr_list[j][wr_index[j] - 1].next = nullptr;
      wr_list[j][wr_index[j] - 1].send_flags = IBV_SEND_SIGNALED;
      wr_list[j][wr_index[j] - 1].wr_id = (uint64_t)wr_context;
    }
    wr_context->ref_++;
    if (ibv_post_send(connections_[j]->GetQp(), wr_list[j], &bad_wr)) {
      PLOG(ERROR) << "ibv_post_send() failed for last batch";
      return -1;
    }
  }
  LOG(INFO) << "PUT finished";
  return 0;
}

int RdmaKVStore::Delete(const std::vector<Key>& keys, const Callback& cb) {
  return 0;
}

int RdmaKVStore::Register(char* buf, size_t size, uint32_t* lkey,
                          uint32_t* rkey) {
  if (!buf) {
    LOG(ERROR) << "Register nullptr. Failed.";
    return -1;
  }
  auto iter = memory_regions_.find((uint64_t)buf);
  if (iter != memory_regions_.end()) {
    // This memory has been registered before.
    *lkey = iter->second->lkey;
    *rkey = iter->second->rkey;
    return 0;
  }
  auto mr = manager_->RegisterMemory(buf, size);
  if (!mr) return -1;
  *lkey = mr->lkey;
  *rkey = mr->rkey;
  memory_regions_.insert({(uint64_t)buf, mr});
  return 0;
}

int RdmaKVServer::Init() {
  manager_ = new RdmaManager(FLAGS_dev);
  connections_.resize(kMaxConcur, nullptr);
  if (manager_->InitDevice()) {
    LOG(ERROR) << "RdmaKVServer::InitDevice() failed";
    return -1;
  }
  if (manager_->InitMemory()) {
    LOG(ERROR) << "RdmaKVServer::InitMemory() failed";
    return -1;
  }
  return 0;
}

int RdmaKVServer::TcpListen() {
  struct addrinfo *res, *t;
  struct addrinfo hints;
  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_flags = AI_PASSIVE;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  char* service;
  int connfd;
  int sockfd = -1, err, n;
  if (asprintf(&service, "%d", FLAGS_port) < 0) return -1;
  if (getaddrinfo(nullptr, service, &hints, &res)) {
    LOG(ERROR) << gai_strerror(n) << " for port " << FLAGS_port;
    free(service);
    return -1;
  }
  for (t = res; t; t = t->ai_next) {
    sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
    if (sockfd >= 0) {
      n = 1;
      setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n));
      if (!bind(sockfd, t->ai_addr, t->ai_addrlen)) break;
      close(sockfd);
      sockfd = -1;
    }
  }
  freeaddrinfo(res);
  free(service);
  if (sockfd < 0) {
    LOG(ERROR) << "Couldn't listen to port " << FLAGS_port;
    return -1;
  }
  LOG(INFO) << "About to listen on port " << FLAGS_port;
  err = listen(sockfd, 1024);
  if (err) {
    PLOG(ERROR) << "listen() failed";
    return -1;
  }
  LOG(INFO) << "Server listen thread starts";
  int idx = 0;
  while (idx < kMaxConcur) {
    connfd = accept(sockfd, nullptr, 0);
    if (connfd < 0) {
      PLOG(ERROR) << "Accept Error";
      break;
    }
    // When connection comes,
    // [TODO] connection handler
    std::thread handler =
        std::thread(&RdmaKVServer::AcceptHander, this, connfd, idx);
    handler.detach();
    idx++;
  }
  // The loop shall never end.
  close(sockfd);
  return -1;
}

int RdmaKVServer::AcceptHander(int fd, int idx) {
  RdmaConnection* conn = manager_->TcpServe(fd);
  connections_[idx] = conn;
  conn->SetIdx(idx);
  int to_post = fLI::FLAGS_recv_wq_depth;
  while (to_post > 0) {
    int batch_size = std::min(to_post, kMaxBatch);
    if (PostRecvBatch(idx, batch_size) < 0) {
      LOG(ERROR) << "PostRecvBatch() failed";
      return -1;
    }
    to_post -= batch_size;
  }
  manager_->TcpAck(fd, conn->GetQp());
  return 0;
}

// n should be strictly less than kMaxBatch
int RdmaKVServer::PostRecvBatch(int conn_id, int n) {
  struct ibv_sge sg[kMaxBatch];
  struct ibv_recv_wr wr[kMaxBatch];
  struct ibv_recv_wr* bad_wr;
  for (int i = 0; i < n; i++) {
    auto rdma_buffer = manager_->AllocateBuffer(FLAGS_buf_size);
    sg[i].addr = rdma_buffer->addr_;
    sg[i].length = rdma_buffer->size_;
    sg[i].lkey = rdma_buffer->lkey_;
    memset(&wr[i], 0, sizeof(struct ibv_recv_wr));
    wr[i].num_sge = 1;
    wr[i].sg_list = &sg[i];
    wr[i].next = (i == n - 1) ? nullptr : &wr[i + 1];
    ServerWrContext* wr_context = new ServerWrContext;
    wr_context->conn_id_ = conn_id;
    wr_context->rdma_buffer_ = rdma_buffer;
    wr[i].wr_id = (uint64_t)wr_context;
  }
  if (auto ret = ibv_post_recv(connections_[conn_id]->GetQp(), wr, &bad_wr)) {
    PLOG(ERROR) << "ibv_post_recv() failed";
    LOG(ERROR) << "Return value is " << ret;
    return -1;
  }
  return 0;
}

}  // namespace kvstore