#ifndef KVSTORE_RDMATOOLS_HPP
#define KVSTORE_RDMATOOLS_HPP
#include <infiniband/verbs.h>

#include <map>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "config.hpp"

namespace kvstore {

class RdmaBuffer {
 public:
  uint64_t addr_;
  uint32_t size_;
  uint32_t lkey_;
  uint32_t rkey_;
  RdmaBuffer(uint64_t addr, uint32_t size, uint32_t lkey, uint32_t rkey)
      : addr_(addr), size_(size), lkey_(lkey), rkey_(rkey) {}
};

class RdmaMemory {
 private:
  struct ibv_mr* mr_ = nullptr;
  struct ibv_pd* pd_ = nullptr;
  // The size should be 4KB, 8KB, 64KB, 1MB
  std::queue<RdmaBuffer*> buffers_;
  size_t size_ = -1;
  size_t ShapeSize(size_t size);

 public:
  RdmaMemory(struct ibv_pd* pd, size_t size) : pd_(pd), size_(size) {}
  int Malloc(int num);
  RdmaBuffer* GetBuffer(size_t size);
  void ReturnBuffer(RdmaBuffer* buf);
  bool MatchBuffer(RdmaBuffer* buf);
};

class RdmaConnection {
 public:
  RdmaConnection(struct ibv_qp* qp) : qp_(qp) {}
  void SetIdx(int id) { idx_ = id; }
  struct ibv_qp* GetQp() {
    return qp_;
  }

 private:
  struct ibv_qp* qp_;
  int idx_;
  int send_credits_ = FLAGS_send_wq_depth;
  int recv_credits_ = FLAGS_recv_wq_depth;
  // TODO: add stats info
};

class RdmaManager {
 public:
  RdmaManager(std::string devname) : devname_(devname) {}
  int InitDevice();
  int InitMemory();
  int TcpConnect(std::string host, int port);  // return sockfd on success.
  // TcpServe sets up the RDMA connection and return the RDMA connection.
  // But TcpServe will still keeps the TCP connection alive (without response to
  // client) TcpAck is used to respond to client and close the TCP connection.
  RdmaConnection* TcpServe(int fd);
  void TcpAck(int fd, struct ibv_qp* qp);
  // Connect to certain host with tcp port "port", set up an RDMA connection and
  // return that back.
  RdmaConnection* Connect(std::string host, int port);
  RdmaBuffer* AllocateBuffer(size_t size);
  void FreeBuffer(RdmaBuffer* buf);
  struct ibv_mr* RegisterMemory(char* buf, size_t size);

 private:
  std::string devname_ = "";
  union ibv_gid gid_;
  struct ibv_context* ctx_ = nullptr;
  struct ibv_pd* pd_ = nullptr;
  std::vector<RdmaMemory*> memory_pools_;
  struct ibv_cq* global_cq_ = nullptr;
  struct ibv_comp_channel* global_channel_ = nullptr;
  class ConnectionMeta {
   public:
    int qp_num;
    union ibv_gid gid;
  };
};

struct ibv_qp_init_attr MakeQpInitAttr(struct ibv_cq* send_cq,
                                       struct ibv_cq* recv_cq);

struct ibv_qp_attr MakeQpAttr(enum ibv_qp_state state, enum ibv_qp_type qp_type,
                              int remote_qpn, const union ibv_gid& remote_gid,
                              int* attr_mask);

}  // namespace kvstore

#endif  // KVSTORE_KVSTORE_H_
