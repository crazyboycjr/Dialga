#ifndef DIALGA_KVSERVER_TCP_HPP_
#define DIALGA_KVSERVER_TCP_HPP_
#include "dialga/kvstore.hpp"
#include "dialga/sarray.hpp"
#include "prism/thread_proto.h"
#include "prism/spsc_queue.h"
#include "dialga/internal/concurrentqueue.hpp"
#include "socket/socket.h"
#include "socket/buffer.h"
#include "socket/poll.h"
#include "./io_worker.hpp"

#include <map>
#include <queue>
#include <memory>
#include <atomic>

namespace dialga {

enum Operation : uint32_t {
  PUT = 1,
  GET = 2,
  DELETE = 3,
};

// On wire data.
// meta | key 0 | key 1 | ... | len 0 | len 1 | ... | value 0 | value 1 | ...
struct KVMeta {
  Operation op;
  uint32_t num_keys;
};

static_assert(sizeof(KVMeta) == 8, "sizeof(KVMeta) != 8");

struct KVPairs {
  SArray<Key> keys;
  SArray<uint32_t> lens;
  std::vector<SArray<char>> values;

  explicit KVPairs() {}

  explicit KVPairs(const KVMeta& meta) {
    if (meta.op == Operation::PUT) {
      // allocate the space for PUT operation
      keys = SArray<Key>(meta.num_keys);
      lens = SArray<uint32_t>(meta.num_keys);
      values.clear();
      values.reserve(meta.num_keys);
      for (uint32_t i = 0; i < meta.num_keys; i++) {
        values[i] = SArray<char>(lens[i] / sizeof(char));
      }
    } else if (meta.op == Operation::GET || meta.op == Operation::DELETE) {
      // for GET and DELETE, only `keys` is non-empty.
      keys = SArray<Key>(meta.num_keys);
      lens.clear();
      values.clear();
    } else {
      CHECK(0) << "unexpected op: " << static_cast<uint32_t>(meta.op);
    }
  }
};

namespace server {
using namespace socket;

class Endpoint;

struct Message {
  Operation op;
  Endpoint* endpoint;
  KVPairs kvs;
};

using WorkQueue = moodycamel::ConcurrentQueue<Message>;

enum class TxStage {
  // NothingToSend doesn't the TxQueue is empty. It just means the current
  // tx_buffer_ is pointing to nothing.
  NothingToSend = 0,
  Meta = 1,
  Keys = 2,
  Lens = 3,
  Values = 4,
};

enum class RxStage {
  Meta = 1,  // start from 1 for debugging
  Keys = 2,
  Lens = 3,
  Values = 4,
};

// class RxStage {
//  public:
//   enum { Meta = 1, Keys, Lens, Values } stage;
//   // only be valid when stage == Values
//   uint32_t value_index;
//
//   RxStage NextStage(Operation op) {
//
//     if (op == Operation::PUT) {
//
//     }
//   }
// };

class Endpoint {
 public:
  using TxQueue = prism::SpscQueue<Message>;

  Endpoint(TcpSocket sock, ioworker::IoWorker<Endpoint>& io_worker);

  void OnError();
  void OnEstablished();
  void OnSendReady();
  void OnRecvReady();

  inline RawFd fd() const { return sock_; }

  inline const TcpSocket& sock() const { return sock_; }
  inline TcpSocket& sock() { return sock_; }

  inline TxQueue& tx_queue() { return tx_queue_; }

  void NotifyWork();

  WorkQueue& GetWorkQueue();

  SockAddr GetPeerAddr();

 private:
  // /*! \brief receive meta */
  // bool ReceiveMeta();
  // /*! \brief receive keys */
  // bool ReceiveKeys();
  // /*! \brief receive the array of the lengths of the values */
  // bool ReceiveLens();
  // /*! \brief receive values */
  // bool ReceiveValues();

  void PrepareNewReceive();
  void PrepareNewSend(const Message& msg);

  TcpSocket sock_;
  ioworker::IoWorker<Endpoint>& io_worker_;
  TxQueue tx_queue_;
  Interest interest_;

  /*! \brief the current tranmission stage */
  TxStage tx_stage_;
  /*! \brief the current index of the value, only be meaningful when the stage
   * is Values */
  uint32_t tx_value_index_;
  /*! \brief a view to an element in TxQueue */
  Buffer tx_buffer_;
  /*! \brief current tranmitting meta */
  KVMeta tx_meta_;
  /*! \brief current tranmitting kvpairs */
  KVPairs tx_kvs_;

  /*! \brief the current receiving stage */
  RxStage rx_stage_;
  /*! \brief the current index of the value, only be meaningful when the stage
   * is Values */
  uint32_t rx_value_index_;
  /*! \brief the receiving buffer, it is always a view of some other buffer */
  Buffer rx_buffer_;
  /*! \brief current receiving meta */
  KVMeta meta_;
  /*! \brief current receiving kvpairs */
  KVPairs kvs_;
};
}  // namespace server

class KVServerTcp final : public KVServer {
 public:
  virtual ~KVServerTcp() {}

  int Init() override;

  int Run() override;

  static int ExitHandler(int sig, void* app_ctx);

  inline server::WorkQueue& work_queue() { return work_queue_; }

 private:
  void MainLoop();

  std::atomic<bool> terminated_;
  server::WorkQueue work_queue_;
  std::vector<std::unique_ptr<ioworker::IoWorker<server::Endpoint>>>
      io_workers_;

  std::unordered_map<Key, SArray<char>> storage_;
};

}  // namespace dialga

#endif  // DIALGA_KVSERVER_TCP_HPP_
