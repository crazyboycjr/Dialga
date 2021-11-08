#include <infiniband/verbs.h>
#include <thread>

#include "kvstore.hpp"
#include "concurrentqueue.hpp"
#include "rdmatools.hpp"
namespace kvstore {

enum KVOpType { KV_PUT = 0, KV_GET, KV_DELETE };
enum AckType { KV_MSG_PUT, KV_MSG_GET };

class TxMessage {
 public:
  Key key_;
  enum KVOpType opcode_;
  // uint64_t size;  Should be compute directly from the wr.length -
  // sizeof(TxMessage);
};

class AckMessage {
 public:
  Key key_;
  uint64_t batch_id_;
  uint64_t addr_;
  uint32_t rkey_;
  enum AckType type_;
  AckMessage(Key key, uint64_t batch_id, uint64_t addr, uint32_t rkey,
             enum AckType type)
      : key_(key), batch_id_(batch_id), addr_(addr), rkey_(rkey), type_(type) {}
};

class RecvWrContext {
 public:
  int conn_id_;              // qp of this wr is in connections_[conn_id_]
  RdmaBuffer* rdma_buffer_;  // the corresponding RdmaBuffer
};

class RdmaKVStore : public KVStore {
 public:
  int Init();  // return 0 on success. If success, then can Put and Get.
  void Poll();
  std::vector<std::string> GetHostList(const std::string&);
  void ParseHostPort(const std::string&, std::string&, int&);
  int PostRecvBatch(int conn_id, int n);
  int Put(const std::vector<Key>& keys, const std::vector<Value>& values);
  int Get(const std::vector<Key>& keys, const std::vector<Value*>& values,
          const Callback& cb = nullptr);
  // TODO: ibv_reg_mr() does not support const char *buf register
  // TODO: When register, allocate size + control message size.
  int Register(char* buf, size_t size);
  int Delete(const std::vector<Key>& keys, const Callback& cb = nullptr);

 private:
  RdmaManager* manager_ = nullptr;
  std::thread polling_thread_;
  std::vector<RdmaConnection*> connections_;
  std::unordered_map<uint64_t, struct ibv_mr*> memory_regions_;
  int recv_debug_cnt_ = 0;
  int send_debug_cnt_ = 0;
  class SendWrContext {
   public:
    int conn_id_;
    int ref_ = 0;
    SendWrContext(int conn_id, int ref) : conn_id_(conn_id), ref_(ref){}
  };
};

class RdmaKVServer {
 public:
  int Init();
  int TcpListen();
  int PollThread();
  int ProcessThread();

 private:
  class StorageEntry {
   public:
    StorageEntry() { value_ = new Value; }
    Value* value_;
    RdmaBuffer* rdma_buffer_;
    ~StorageEntry() {
      delete value_;
    }
  };
  RdmaManager* manager_ = nullptr;
  std::vector<RdmaConnection*> connections_;
  std::unordered_map<uint64_t, StorageEntry*> storage_;  // Key to Value storage
  moodycamel::ConcurrentQueue<struct ibv_wc> wc_queues_;
  std::thread polling_thread_;
  std::thread process_thread_;
  int recv_debug_cnt_ = 0;
  int send_debug_cnt_ = 0;
  
  int AcceptHander(int fd, int idx);
  // Post n RdmaBuffer to the QP for receiver
  // n RdmaBuffer is from manager_;
  int PostRecvBatch(int conn_id, int n);
  int ProcessRecvCqe(struct ibv_wc* wc);
  int ProcessPut(int conn_id, char* msg_buf, size_t msg_size, TxMessage* msg,
                 uint32_t put_id);
  int PostSend(int conn_id, RdmaBuffer* rdma_buffer, size_t size,
               uint32_t imm_data);
};
}  // namespace kvstore