#include <infiniband/verbs.h>

#include <thread>
#include <vector>

#include "concurrentqueue.hpp"
#include "kvstore.hpp"
#include "rdmatools.hpp"
namespace kvstore {

enum KVOpType { KV_PUT = 0, KV_GET, KV_DELETE };
enum AckType { KV_ACK_PUT = 0, KV_ACK_GET };

class TxMessage {
 public:
  Key key_;
  enum KVOpType opcode_;
  // For GET() op
  uint64_t get_ctx_ptr_;
  // uint64_t size;  Should be compute directly from the wr.length -
  // sizeof(TxMessage);
};

class GetContext {
 public:
  int* ref_ptr_;
  Value* value_ptr_;
  uint32_t batch_id_;
  uint32_t key_id_;
  Callback cb_;

  GetContext(int* ref_ptr, Value* value_ptr, uint32_t batch_id, uint32_t key_id,
             Callback cb)
      : ref_ptr_(ref_ptr),
        value_ptr_(value_ptr),
        batch_id_(batch_id),
        key_id_(key_id),
        cb_(cb) {}
};

class AckMessage {
 public:
  Key key_;
  uint64_t get_ctx_ptr_;
  uint64_t addr_;
  size_t size_;
  uint32_t rkey_;
  enum AckType type_;
  AckMessage(Key key, uint64_t get_ctx_ptr, uint64_t addr, size_t size,
             uint32_t rkey, enum AckType type)
      : key_(key),
        get_ctx_ptr_(get_ctx_ptr),
        addr_(addr),
        size_(size),
        rkey_(rkey),
        type_(type) {}
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
  int PrepostProcess(
      const std::vector<Key>& keys, const std::vector<Value>& values,
      bool create,  // create differs GET/PUT. If create is true, when entry
                    // lookup failed, create a new entry
      std::vector<int>& output_conn_ids);
  // Didn't find good name.
  // PrepostProcess: 1. match each key to the corresponding connection and
  // return the vector of the connection idx (through &)
  //                 2. post recv requests for each connection.
  int ProcessPutAck(struct ibv_wc* wc);
  int ProcessGetAck(struct ibv_wc* wc);
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
  uint32_t get_id_ = 0;
  class SendWrContext {
   public:
    int conn_id_;
    int ref_ = 0;
    SendWrContext(int conn_id, int ref) : conn_id_(conn_id), ref_(ref) {}
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
    ~StorageEntry() { delete value_; }
  };
  RdmaManager* manager_ = nullptr;
  std::vector<RdmaConnection*> connections_;
  std::unordered_map<uint64_t, StorageEntry*> storage_;  // Key to Value storage
  moodycamel::ConcurrentQueue<struct ibv_wc> wc_queues_;
  std::thread polling_thread_;
  std::thread process_thread_;

  int AcceptHander(int fd, int idx);
  // Post n RdmaBuffer to the QP for receiver
  // n RdmaBuffer is from manager_;
  int PostRecvBatch(int conn_id, int n);
  int ProcessRecvCqe(struct ibv_wc* wc);
  int ProcessPut(int conn_id, char* msg_buf, size_t msg_size, TxMessage* msg,
                 uint32_t imm_data);
  int ProcessGet(int conn_id, TxMessage* tx_msg, uint32_t imm_data);
  int PostSend(int conn_id, RdmaBuffer* rdma_buffer, size_t size,
               uint32_t imm_data);
};
}  // namespace kvstore