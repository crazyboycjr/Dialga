#ifndef DIALGA_KVSTORE_TCP_HPP_
#define DIALGA_KVSTORE_TCP_HPP_
#include "dialga/kvstore.hpp"
#include "dialga/config.hpp"
#include "dialga/range.hpp"
#include "prism/thread_proto.h"
#include "prism/spsc_queue.h"
#include "./socket/socket.h"
#include "./socket/poll.h"
#include "./socket/buffer.h"
#include "./io_worker.hpp"
#include "./wire.hpp"

#include <map>
#include <queue>
#include <memory>
#include <algorithm>
#include <mutex>
#include <atomic>

namespace dialga {

namespace client {
using namespace socket;

struct OpContext {
  /*! \brief the expected number of responses */
  uint32_t expected;
  /*! \brief the number of responses has been received */
  std::atomic<uint32_t> received;
  /*! \brief the callback to run, (Send + Static) */
  Callback cb;

  explicit OpContext(uint32_t target, Callback callback) {
    expected = target;
    received.store(0);
    cb = callback;
  }

  // TODO(cjr): double-check this function.
  inline bool UpdateReceived() {
    auto last = this->received.fetch_add(1);
    return last + 1 == this->expected;
  }
};

struct Message {
  Operation op;
  uint64_t timestamp;
  KVPairs kvs;
  // These two fields are only for GET.
  OpContext* ctx;
  std::vector<Value*> values;
};

struct OpState {
  OpContext* ctx;
  std::vector<Value*> values;
};

enum class TxStage {
  // NothingToSend doesn't mean the TxQueue is empty. It just means the current
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

class Endpoint {
 public:
  using TxQueue = prism::SpscQueue<Message>;

  Endpoint(TcpSocket sock, ioworker::IoWorker<Endpoint>& io_worker);

  ~Endpoint();

  void OnError();
  void OnEstablished();
  void OnSendReady();
  void OnRecvReady();

  inline RawFd fd() const { return sock_; }

  inline const TcpSocket& sock() const { return sock_; }
  inline TcpSocket& sock() { return sock_; }

  inline TxQueue& tx_queue() { return tx_queue_; }

 private:
  void PrepareNewReceive();

  void PrepareNewSend(const Message& msg);

  void AddCallback(const Message& msg);

  void RunCallback(uint64_t timestamp);

  SockAddr GetPeerAddr();

  TcpSocket sock_;
  ioworker::IoWorker<Endpoint>& io_worker_;
  TxQueue tx_queue_;
  Interest interest_;

  // timestamp -> (ctx, values)
  std::unordered_map<uint64_t, OpState>
      ctx_table_;

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

enum class ScheduleMode {
  // For each connection, find an empty IoWorker or spawn a new one for it.
  Dedicate,
  // Do not spawn new IoWorkers, find an existing IoWorker.
  Shared,
};

class IoManager {
 public:
  static IoManager* Get() {
    static IoManager inst;
    return &inst;
  }

  ~IoManager() {
    for (auto& io_worker : io_workers_) {
      io_worker->Terminate();
    }
    for (auto& io_worker : io_workers_) {
      io_worker->Join();
    }
  }

  ioworker::IoWorker<client::Endpoint>* ScheduleIoWorker(ScheduleMode mode) {
    std::lock_guard<std::mutex> lk(mu_);
    // schedule a IoWorker for this connection
    CHECK(!io_workers_.empty());
    if (mode == ScheduleMode::Dedicate) {
      for (const auto& io_worker : io_workers_) {
        if (io_worker->GetNumEndpoints() == 0) {
          return io_worker.get();
        }
      }
      // spawn a new one
      SpawnOne();
      return io_workers_.back().get();
    } else if (mode == ScheduleMode::Shared) {
      auto it =
          std::min_element(std::begin(io_workers_), std::end(io_workers_),
                           [](const auto& a, const auto& b) {
                             return a->GetNumEndpoints() < b->GetNumEndpoints();
                           });
      return it->get();
    }

    CHECK(0) << "Unreachable";
  }

 private:
  void SpawnOne() {
    auto io_worker =
        std::make_unique<ioworker::IoWorker<client::Endpoint>>(nullptr);
    io_worker->Start();
    io_workers_.push_back(std::move(io_worker));
  }
  IoManager() {
    for (uint32_t i = 0; i < FLAGS_num_io_workers; i++) {
      SpawnOne();
    }
  }

  std::mutex mu_;
  /*! \brief the IoWorker instances */
  std::vector<std::unique_ptr<ioworker::IoWorker<client::Endpoint>>>
      io_workers_;
};

}  // namespace client

class KVStoreTcp final : public KVStore {
 public:
  virtual ~KVStoreTcp() {}

  int Init() override;

  int Put(const std::vector<Key>& keys,
          const std::vector<Value>& values,
          const Callback& cb = nullptr) override;

  int Get(const std::vector<Key>& keys,
          std::vector<Value*>& values,
          const Callback& cb = nullptr) override;

  int Delete(const std::vector<Key>& keys,
             const Callback& cb = nullptr) override;

  int Register(const char* buf, size_t size) override;

  void Free(Value* value) override;

 private:
  /*! \brief active connections */
  std::vector<std::shared_ptr<client::Endpoint>> endpoints_;
  /*! \brief logical timestamp to identify a request */
  uint64_t timestamp_;
};



}  // namespace dialga

#endif  // DIALGA_KVSTORE_TCP_HPP_
