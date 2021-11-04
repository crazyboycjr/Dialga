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
  struct ibv_qp* GetQp() {
    return qp_;
  }

 private:
  struct ibv_qp* qp_;
  int idx_;
  // TODO: add stats info
};

class RdmaManager {
 public:
  RdmaManager(std::string devname) : devname_(devname) {}
  int InitDevice();
  int InitMemory();
  int TcpConnect(std::string host, int port);  // return sockfd on success.
  RdmaConnection* TcpServe(int fd);
  // Connect to certain host with tcp port "port", set up an RDMA connection and
  // return that back.
  RdmaConnection* Connect(std::string host, int port);
  RdmaBuffer* GetRdmaBufferByAddr(char *buf);
  char* AllocateBuffer(size_t size);
  void FreeBuffer(char* buf);

 private:
  std::string devname_ = "";
  union ibv_gid gid_;
  struct ibv_context* ctx_ = nullptr;
  struct ibv_pd* pd_ = nullptr;
  std::vector<RdmaMemory*> memory_pools_;
  struct ibv_cq* global_cq_ = nullptr;
  struct ibv_comp_channel* global_channel_ = nullptr;
  std::unordered_map<uint64_t, RdmaBuffer*> allocated_buffers_;
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
