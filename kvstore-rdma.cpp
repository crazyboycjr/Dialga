/*
 * RDMA KVStore Implementation
 */
#include <arpa/inet.h>
#include <gflags/gflags.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <sys/socket.h>

#include <thread>

#include "config.hpp"
#include "kvstore-rdma.hpp"
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
  }
  return 0;
}

int RdmaKVStore::Get(const std::vector<Key>& keys,
                     const std::vector<Value*>& values, const Callback& cb) {
  return 0;
}

int RdmaKVStore::Put(const std::vector<Key>& keys,
                     const std::vector<Value>& values, const Callback& cb) {
  return 0;
}

int RdmaKVStore::Delete(const std::vector<Key>& keys, const Callback& cb) {
  return 0;
}

char* RdmaKVStore::RdmaMalloc(size_t size) {
    auto ret = manager_->AllocateBuffer(size);
    return ret;
}

void RdmaKVStore::RdmaFree(char *buf) {
    if (!buf) return;
    manager_->FreeBuffer(buf);
}

int RdmaKVStore::FakePost(char *buf) {
    auto qp = connections_[0]->GetQp();
    auto rdma_buffer = manager_->GetRdmaBufferByAddr(buf);
    struct ibv_send_wr wr_list[kMaxBatch];
    struct ibv_sge sgs[kMaxBatch][kMaxSge];
    for (uint32_t i = 0; i < 1; i++) {
        sgs[i][0].addr = rdma_buffer->addr_;
        sgs[i][0].length = rdma_buffer->size_;
        sgs[i][0].lkey = rdma_buffer->lkey_;
        memset(&wr_list[i], 0, sizeof(struct ibv_send_wr));
        wr_list[i].num_sge = 1;
        wr_list[i].opcode = IBV_WR_SEND;
        wr_list[i].send_flags = IBV_SEND_SIGNALED;
        wr_list[i].wr_id = 0;
        wr_list[i].sg_list = sgs[i];
        wr_list[i].next = nullptr;
    }
    struct ibv_send_wr *bad_wr = nullptr;
    if (ibv_post_send(qp, wr_list, &bad_wr)) {
        PLOG(ERROR) << "ibv_post_send() failed";
        return -1;
    }
    LOG(INFO) << "ibv_post_send() succeed";
    struct ibv_wc wc[kCqPollDepth];
    // For test. Real applications should return here.
    while (true) {
        int n = ibv_poll_cq(qp->send_cq, kCqPollDepth, wc);
        if (n < 0) {
            PLOG(ERROR) << "ibv_poll_cq() failed";
            return -1;
        }
        for (int i = 0; i < n; i++) {
            LOG(INFO) << "Completion received.";
            if (wc[i].status != IBV_WC_SUCCESS) {
                LOG(ERROR) << "Got bad completion status with " << wc[i].status;
                return -1;
            }
        }
    }
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
  return 0;
}

}  // namespace kvstore