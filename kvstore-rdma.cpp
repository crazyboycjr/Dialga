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

#include <memory>
#include <thread>
#include <unordered_map>

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
  // Client also needs own its RDMA memory for control message.
  if (manager_->InitMemory()) {
    LOG(ERROR) << "RdmaKVStore::InitMemory() failed";
    return -1;
  }
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

int RdmaKVStore::ProcessPutAck(struct ibv_wc* wc) {
  // TODO: update local index hashmap for further write & read
  auto recv_ctx = (RecvWrContext*)wc->wr_id;
  auto ack_msg = (AckMessage*)(recv_ctx->rdma_buffer_->addr_);
  manager_->FreeBuffer(recv_ctx->rdma_buffer_);
  connections_[recv_ctx->conn_id_]->UpdateRecvCredits(1);
  delete recv_ctx;
  return 0;
}

int RdmaKVStore::ProcessGetAck(struct ibv_wc* wc) {
  auto recv_ctx = (RecvWrContext*)wc->wr_id;
  auto ack_msg = (AckMessage*)(recv_ctx->rdma_buffer_->addr_ + wc->byte_len - sizeof(AckMessage));
  auto get_ctx = (GetContext*) ack_msg->get_ctx_ptr_;
  // First, provide the buffer to user
  get_ctx->value_ptr_->addr_ = recv_ctx->rdma_buffer_->addr_;
  get_ctx->value_ptr_->size_ = ack_msg->size_;
  // Second, update recv credits
  connections_[recv_ctx->conn_id_]->UpdateRecvCredits(1);
  // Third, decrease ref_ptr, and check if the callback function should be called.
  *get_ctx->ref_ptr_ = *get_ctx->ref_ptr_ - 1;
  if (*get_ctx->ref_ptr_ == 0) {
    // Call the callback function and destroy the GetContext.
    get_ctx->cb_();
    delete get_ctx->ref_ptr_;
    delete get_ctx;
  }
  delete recv_ctx;
  return 0;
}

void RdmaKVStore::Poll() {
  while (true) {
    for (auto conn : connections_) {
      auto cq =
          conn->GetQp()
              ->send_cq;  // NOTICE: we use the same cq for both send/recv.
      int n = 1;
      SendWrContext* send_ctx;
      while (n > 0) {
        struct ibv_wc wc[kCqPollDepth];
        n = ibv_poll_cq(cq, kCqPollDepth, wc);
        if (n < 0) {
          PLOG(ERROR) << "ibv_poll_cq() failed";
          return;
        }
        for (int i = 0; i < n; i++) {
          if (wc[i].status != IBV_WC_SUCCESS) {
            LOG(ERROR) << "Got bad completion status with " << wc[i].status;
            // TODO: maybe error handling here
            return;
          }
          switch (wc[i].opcode) {
            case IBV_WC_RDMA_WRITE:
            case IBV_WC_RDMA_READ:
            // TODO: more logics for v2
            case IBV_WC_SEND:
              send_ctx = (SendWrContext*)wc[i].wr_id;
              connections_[send_ctx->conn_id_]->UpdateSendCredits(
                  send_ctx->ref_);
              break;
            case IBV_WC_RECV:
              // TODO: PUT the recv buffer value to the user's designated
              switch ((enum KVOpType)ntohl(wc[i].imm_data)) {
                case KV_PUT:
                  if (ProcessPutAck(&wc[i])) {
                    LOG(ERROR) << "ProcessPutAck() failed";
                  }
                  break;
                case KV_GET:
                  if (ProcessGetAck(&wc[i])) {
                    LOG(ERROR) << "ProcessGetAck() failed";
                  }
                  break;
                default:
                  LOG(ERROR) << "Other Type of ack should not be received";
                  break;
              }
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

// n should be strictly less than kMaxBatch
int RdmaKVStore::PostRecvBatch(int conn_id, int n) {
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
    RecvWrContext* wr_context = new RecvWrContext;
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

int RdmaKVStore::PrepostProcess(const std::vector<Key>& keys,
                                const std::vector<Value>& values, bool create,
                                std::vector<int>& output_conn_ids) {
  int to_post = keys.size();
  std::vector<int> wr_per_connection(connections_.size(), 0);
  output_conn_ids.clear();
  for (int i = 0; i < to_post; i++) {
    auto key_index = indexs_.find(keys[i]);
    int conn_idx;
    if (key_index == indexs_.end()) {
      if (!create) {
        LOG(ERROR) << "GET should only access existing keys";
        return -1;
      }
      conn_idx = i % connections_.size();
      IndexEntry newentry(conn_idx, 0, values[i].size_);
      indexs_.insert({keys[i], newentry});
    } else {
      conn_idx = key_index->second.qp_index_;
    }
    output_conn_ids.push_back(conn_idx);
    wr_per_connection[conn_idx]++;
  }
  for (size_t i = 0; i < wr_per_connection.size(); i++) {
    if (wr_per_connection[i] > 0) {
      bool recv_ready = false, send_ready = false;
      while (true) {
        recv_ready = connections_[i]->AcquireRecvCredits(wr_per_connection[i]);
        send_ready = connections_[i]->AcquireSendCredits(wr_per_connection[i]);
        if (send_ready && recv_ready) break;
        if (send_ready)
          connections_[i]->UpdateSendCredits(
              wr_per_connection[i]);  // avoid deadlock
        if (recv_ready)
          connections_[i]->UpdateRecvCredits(
              wr_per_connection[i]);  // avoid deadlock
      }
      if (PostRecvBatch(i, wr_per_connection[i]) < 0) {
        LOG(ERROR) << "RdmaKVStore client post recv batch failed";
        return -1;
      }
    }
  }
  return 0;
}

int RdmaKVStore::Get(const std::vector<Key>& keys,
                     const std::vector<Value*>& values, const Callback& cb) {
  if (keys.size() != values.size()) {
    LOG(ERROR) << "Get() failed due to size mismatch (key & value)";
    return -1;
  }
  int to_post = keys.size();
  int get_id = get_id_++;
  std::vector<int> conn_ids;
  if (PrepostProcess(keys, {}, false, conn_ids)) {
    LOG(ERROR) << "PrepostProcess for GET failed";
    return -1;
  }
  struct ibv_send_wr wr_list[kMaxConnection][kMaxBatch];
  struct ibv_send_wr* bad_wr;
  struct ibv_sge sgs[kMaxConnection][kMaxBatch];
  int wr_index[kMaxConnection];
  memset(wr_index, 0, sizeof(wr_index));
  memset(wr_list, 0, sizeof(wr_list));
  memset(sgs, 0, sizeof(sgs));
  int i = 0;
  int* ref_ptr = new int;
  *ref_ptr = to_post;
  while (i < to_post) {
    // Generate WR at right position
    int conn_idx = conn_ids[i];
    int wr_position = wr_index[conn_idx];
    wr_index[conn_idx]++;
    auto rdma_buffer = manager_->AllocateBuffer(sizeof(TxMessage));
    GetContext* get_ctx = new GetContext(ref_ptr, values[i], get_id, i, cb);
    TxMessage* msg_buffer = (TxMessage*)(rdma_buffer->addr_);
    msg_buffer->key_ = keys[i];
    msg_buffer->opcode_ = KV_GET;
    msg_buffer->get_ctx_ptr_ = (uint64_t)get_ctx;
    sgs[conn_idx][wr_position].addr = rdma_buffer->addr_;
    sgs[conn_idx][wr_position].length = sizeof(TxMessage);
    // Message Format: [[control message]
    sgs[conn_idx][wr_position].lkey = rdma_buffer->lkey_;
    wr_list[conn_idx][wr_position].num_sge = 1;
    wr_list[conn_idx][wr_position].imm_data = htonl((uint32_t)KV_GET);
    wr_list[conn_idx][wr_position].sg_list = &sgs[conn_idx][wr_position];
    wr_list[conn_idx][wr_position].opcode = IBV_WR_SEND_WITH_IMM;
    i++;
    if (wr_index[conn_idx] == kMaxBatch) {
      // This batch should be sent out
      wr_list[conn_idx][wr_position].next = nullptr;
      wr_list[conn_idx][wr_position].send_flags = IBV_SEND_SIGNALED;
      SendWrContext* wr_ctx = new SendWrContext(conn_idx, kMaxBatch);
      wr_list[conn_idx][wr_position].wr_id = (uint64_t)wr_ctx;
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
      SendWrContext* wr_ctx = new SendWrContext(j, wr_index[j]);
      wr_list[j][wr_index[j] - 1].wr_id = (uint64_t)wr_ctx;
    }
    if (ibv_post_send(connections_[j]->GetQp(), wr_list[j], &bad_wr)) {
      PLOG(ERROR) << "ibv_post_send() failed for last batch";
      return -1;
    }
  }
  return 0;
}

int RdmaKVStore::Put(const std::vector<Key>& keys,
                     const std::vector<Value>& values) {
  // TODO: generate a operation id that denotes this batch
  if (keys.size() != values.size()) {
    LOG(ERROR) << "Put() failed due to size mismatch (key & value)";
    return -1;
  }
  // Before posting this PUT, we need to post enough receive requests for ACK
  // msg.
  std::vector<int> conn_ids;
  if (PrepostProcess(keys, values, true, conn_ids)) {
    LOG(ERROR) << "PrepostProcess for PUT failed";
    return -1;
  }
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
    // Generate WR at right position
    int conn_idx = conn_ids[i];
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
    if (sgs[conn_idx][wr_position].length > iter->second->length) {
      LOG(ERROR) << "Value's length " << sgs[conn_idx][wr_position].length
                 << " exceed registered buffer size " << iter->second->length;
      return -1;
    }
    // Message Format: [real value][control message]
    sgs[conn_idx][wr_position].lkey = iter->second->lkey;
    wr_list[conn_idx][wr_position].num_sge = 1;
    wr_list[conn_idx][wr_position].sg_list = &sgs[conn_idx][wr_position];
    wr_list[conn_idx][wr_position].opcode = IBV_WR_SEND_WITH_IMM;
    wr_list[conn_idx][wr_position].imm_data = htonl((uint32_t)KV_PUT);
    i++;
    if (wr_index[conn_idx] == kMaxBatch) {
      // This batch should be sent out
      wr_list[conn_idx][wr_position].next = nullptr;
      wr_list[conn_idx][wr_position].send_flags = IBV_SEND_SIGNALED;
      SendWrContext* wr_ctx = new SendWrContext(conn_idx, kMaxBatch);
      wr_list[conn_idx][wr_position].wr_id = (uint64_t)wr_ctx;
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
      SendWrContext* wr_ctx = new SendWrContext(j, wr_index[j]);
      wr_list[j][wr_index[j] - 1].wr_id = (uint64_t)wr_ctx;
    }
    if (ibv_post_send(connections_[j]->GetQp(), wr_list[j], &bad_wr)) {
      PLOG(ERROR) << "ibv_post_send() failed for last batch";
      return -1;
    }
  }
  return 0;
}

int RdmaKVStore::Delete(const std::vector<Key>& keys, const Callback& cb) {
  return 0;
}

int RdmaKVStore::Register(char* buf, size_t size) {
  size += kCtrlMsgSize;
  if (!buf) {
    LOG(ERROR) << "Register nullptr. Failed.";
    return -1;
  }
  auto iter = memory_regions_.find((uint64_t)buf);
  if (iter != memory_regions_.end()) {
    // This memory has been registered before.
    return 0;
  }
  auto mr = manager_->RegisterMemory(buf, size);
  if (!mr) return -1;
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
  // Starting from here.
  polling_thread_ = std::thread(&RdmaKVServer::PollThread, this);
  polling_thread_.detach();
  process_thread_ = std::thread(&RdmaKVServer::ProcessThread, this);
  process_thread_.detach();
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
    if (!connections_[idx]->AcquireRecvCredits(batch_size)) {
      LOG(ERROR) << "First batch should be posted";
    }
    if (PostRecvBatch(idx, batch_size) < 0) {
      LOG(ERROR) << "PostRecvBatch() failed";
      return -1;
    }
    to_post -= batch_size;
  }
  manager_->TcpAck(fd, conn->GetQp());
  return 0;
}

int RdmaKVServer::ProcessThread() {
  LOG(INFO) << "RdmaKVServer Process thread starts";
  while (true) {
    struct ibv_wc wc;
    bool success = wc_queues_.try_dequeue(wc);
    if (success) {
      // Dequeue success
      if (ProcessRecvCqe(&wc)) {
        LOG(ERROR) << "ProcessRecvCqe() failed in processing thread";
      }
      delete (RecvWrContext*)wc.wr_id;
    }
  }
}

int RdmaKVServer::PollThread() {
  // It process all CQs and repsond to client accordingly
  LOG(INFO) << "RdmaKVServer Poll thread starts";
  while (true) {
    for (auto conn : connections_) {
      if (!conn) continue;
      auto cq =
          conn->GetQp()->send_cq;  // send_cq and recv_cq are the same here
      int n = 1;
      while (n > 0) {
        struct ibv_wc wc[kCqPollDepth];
        n = ibv_poll_cq(cq, kCqPollDepth, wc);
        if (n < 0) {
          PLOG(ERROR) << "ibv_poll_cq() failed";
          return -1;
        }
        for (int i = 0; i < n; i++) {
          if (wc[i].status != IBV_WC_SUCCESS) {
            LOG(ERROR) << "Got bad completion status with " << wc[i].status;
            // TODO: maybe error handling here
            return -1;
          }
          auto wr_context = (RecvWrContext*)wc[i].wr_id;
          switch (wc[i].opcode) {
            case IBV_WC_RDMA_WRITE:
            case IBV_WC_RDMA_READ:
              LOG(ERROR) << "Server won't issue WRITE/READ. ERROR.";
              break;
            case IBV_WC_SEND:
              // Update Send credits and put buffer back to pool
              connections_[wr_context->conn_id_]->UpdateSendCredits(1);
              manager_->FreeBuffer(wr_context->rdma_buffer_);
              delete wr_context;
              break;
            case IBV_WC_RECV:
              connections_[wr_context->conn_id_]->UpdateRecvCredits(1);
              // put the ibv_wc to the concurrent queue. The process thread will
              // process it.
              wc_queues_.enqueue(wc[i]);
              break;
            default:
              LOG(ERROR) << "Other Ops are not supported. Must be error.";
              break;
          }
        }
      }
    }
  }
  // Never reach here
  return 0;
}

int RdmaKVServer::ProcessRecvCqe(struct ibv_wc* wc) {
  auto wr_ctx = (RecvWrContext*)wc->wr_id;
  auto rdma_buffer = wr_ctx->rdma_buffer_;
  char* buf = (char*)rdma_buffer->addr_;
  TxMessage* tx_msg = (TxMessage*)(buf + wc->byte_len - sizeof(TxMessage));
  // LOG(INFO) << "ProcessRecvCqe: rdma_buffer size is " << rdma_buffer->size_;
  // LOG(INFO) << "ProcessRecvCqe: wc->byte_len is " << wc->byte_len
  //           << " , sizeof(TxMessage) is " << sizeof(TxMessage);
  // LOG(INFO) << "The imm_data is " << wc->imm_data << " , ntohl(imm_data) is " << ntohl(wc->imm_data);
  std::unordered_map<uint64_t, StorageEntry*>::iterator iter;
  switch (tx_msg->opcode_) {
    case KV_PUT:
      if (ProcessPut(wr_ctx->conn_id_, buf, wc->byte_len - sizeof(TxMessage),
                     tx_msg, ntohl(wc->imm_data))) {
        LOG(ERROR) << "ProcessPut message failed";
        return -1;
      }
      break;
    case KV_GET:
      if (ProcessGet(wr_ctx->conn_id_, tx_msg, ntohl(wc->imm_data))) {
        LOG(ERROR) << "ProcessGet message failed";
        return -1;
      }
      break;
    default:
      break;
  }
  manager_->FreeBuffer(rdma_buffer);
  int batch_size = kMaxBatch / 2;
  if (connections_[wr_ctx->conn_id_]->AcquireRecvCredits(batch_size)) {
    // update recv wqes in a batch
    if (PostRecvBatch(wr_ctx->conn_id_, batch_size)) {
      LOG(ERROR) << "PostRecvBatch(" << wr_ctx->conn_id_ << " , " << batch_size
                 << ") failed";
      return -1;
    }
  }
  return 0;
}

int RdmaKVServer::ProcessGet(int conn_id, TxMessage* tx_msg,
                             uint32_t imm_data) {
  auto iter = storage_.find(tx_msg->key_);
  if (iter == storage_.end()) {  // This should not happen.
    LOG(ERROR) << "KEY error: " << tx_msg->key_ << " doesn't exist";
    return -1;  // TODO: respond back to client
  }
  auto entry = iter->second;
  // Then, send back this entry
  char* buf = (char*)entry->rdma_buffer_->addr_;
  AckMessage* ack_msg = (AckMessage*)(buf + entry->value_->size_);
  ack_msg->addr_ = entry->rdma_buffer_->addr_;
  ack_msg->get_ctx_ptr_ = tx_msg->get_ctx_ptr_;
  ack_msg->key_ = tx_msg->key_;
  ack_msg->rkey_ = entry->rdma_buffer_->rkey_;
  ack_msg->type_ = KV_ACK_GET;
  ack_msg->size_ = entry->value_->size_;
  if (PostSend(conn_id, entry->rdma_buffer_,
               entry->value_->size_ + sizeof(AckMessage), imm_data)) {
    LOG(ERROR)
        << "RdmaKVServer::ProcessGet() failed when PostSend back ACK message";
    return -1;
  }
  return 0;
}

int RdmaKVServer::ProcessPut(int conn_id, char* msg_buf, size_t msg_size,
                             TxMessage* tx_msg, uint32_t imm_data) {
  auto iter = storage_.find(tx_msg->key_);
  uint64_t addr;
  uint32_t rkey;
  if (iter == storage_.end()) {  // New PUT (CREATE)
    auto new_buffer = manager_->AllocateBuffer(msg_size);
    auto entry = new StorageEntry;
    addr = new_buffer->addr_;
    rkey = new_buffer->rkey_;
    entry->rdma_buffer_ = new_buffer;
    entry->value_->addr_ = addr;
    entry->value_->size_ = msg_size;
    storage_.insert({tx_msg->key_, entry});
  } else {
    auto entry = iter->second;
    addr = entry->value_->addr_;
    rkey = entry->rdma_buffer_->rkey_;
  }
  memcpy((void*)addr, msg_buf, msg_size);
  auto send_buffer = manager_->AllocateBuffer(sizeof(AckMessage));
  AckMessage* ack_msg = (AckMessage*)send_buffer->addr_;
  ack_msg->addr_ = addr;
  ack_msg->get_ctx_ptr_ = 0;
  ack_msg->key_ = tx_msg->key_;
  ack_msg->rkey_ = rkey;
  ack_msg->size_ = msg_size;
  ack_msg->type_ = KV_ACK_PUT;
  if (PostSend(conn_id, send_buffer, sizeof(AckMessage), imm_data)) {
    LOG(ERROR)
        << "RdmaKVServer::ProcessPut() failed when PostSend back ACK message";
    return -1;
  }
  return 0;
}

int RdmaKVServer::PostSend(int conn_id, RdmaBuffer* rdma_buffer, size_t size,
                           uint32_t imm_data) {
  RecvWrContext* wr_context = new RecvWrContext;
  wr_context->conn_id_ = conn_id;
  // TODO: what is better when credits currently not available?
  while (!connections_[conn_id]->AcquireSendCredits(1))
    LOG(INFO) << "RdmaKVStore no send credits";
  struct ibv_send_wr wr;
  struct ibv_send_wr* bad_wr;
  struct ibv_sge sg;
  memset(&wr, 0, sizeof(wr));
  sg.addr = rdma_buffer->addr_;
  sg.length = size;
  sg.lkey = rdma_buffer->lkey_;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_SEND_WITH_IMM;
  wr.sg_list = &sg;
  wr_context->rdma_buffer_ = rdma_buffer;
  wr.wr_id = (uint64_t)
      wr_context;  // We only need to put this buffer back to the memory pool
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.imm_data = htonl(imm_data);
  if (ibv_post_send(connections_[conn_id]->GetQp(), &wr, &bad_wr) < 0) {
    PLOG(ERROR) << "RdmaKVServer::PostSend() ibv_post_send() failed";
    return -1;
  }
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
    RecvWrContext* wr_context = new RecvWrContext;
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