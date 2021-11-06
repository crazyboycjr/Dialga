#include <thread>

#include "kvstore.hpp"
#include "rdmatools.hpp"
namespace kvstore {

enum KVOpType { KV_PUT = 0, KV_GET, KV_DELETE };

class TxMessage {
 public:
  Key key_;
  enum KVOpType opcode_;
  // uint64_t size;  Should be compute directly from the wr.length -
  // sizeof(TxMessage);
};

class RdmaKVStore : public KVStore {
 public:
  int Init();  // return 0 on success. If success, then can Put and Get.
  void Poll();
  std::vector<std::string> GetHostList(const std::string&);
  void ParseHostPort(const std::string&, std::string&, int&);
  int Put(const std::vector<Key>& keys, const std::vector<Value>& values,
          const Callback& cb = nullptr);
  int Get(const std::vector<Key>& keys, const std::vector<Value*>& values,
          const Callback& cb = nullptr);
  // TODO: ibv_reg_mr() does not support const char *buf register
  // TODO: When register, allocate size + control message size.
  int Register(char* buf, size_t size, uint32_t* lkey, uint32_t* rkey);
  int Delete(const std::vector<Key>& keys, const Callback& cb = nullptr);

 private:
  RdmaManager* manager_ = nullptr;
  std::thread polling_thread_;
  std::vector<RdmaConnection*> connections_;
  std::unordered_map<uint64_t, struct ibv_mr*> memory_regions_;
  class WrContext {
   public:
    int ref_ = 0;
    Callback cb_;
    WrContext(int ref, const Callback& cb = nullptr) : ref_(ref), cb_(cb) {}
  };
};

class RdmaKVServer {
 public:
  int Init();
  int TcpListen();
  int ProcessThread();

 private:
  RdmaManager* manager_ = nullptr;
  std::vector<RdmaConnection*> connections_;
  std::unordered_map<uint64_t, Value*> storage_;  // Key to Value storage

  int AcceptHander(int fd, int idx);
  // Post n RdmaBuffer to the QP for receiver
  // n RdmaBuffer is from manager_;
  int PostRecvBatch(int conn_id, int n);
  class ServerWrContext {
   public:
    int conn_id_;              // qp of this wr is in connections_[conn_id_]
    RdmaBuffer* rdma_buffer_;  // the corresponding RdmaBuffer
  };
};
}  // namespace kvstore