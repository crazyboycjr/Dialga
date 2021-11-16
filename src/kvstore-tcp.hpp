#ifndef DIALGA_KVSTORE_TCP_HPP_
#define DIALGA_KVSTORE_TCP_HPP_
#include "dialga/kvstore.hpp"
#include "prism/thread_proto.h"
#include "socket/socket.h"
#include "socket/poll.h"
#include "./io_worker.hpp"

#include <map>
#include <queue>
#include <memory>

namespace dialga {

namespace client {
using namespace socket;
class Endpoint {
 public:
  using TxQueue = std::queue<int>;

  Endpoint(TcpSocket sock, ioworker::IoWorker<Endpoint>& io_worker);

  void OnError();
  void OnEstablished();
  void OnSendReady();
  void OnRecvReady();

  inline RawFd fd() const { return sock_; }

  inline const TcpSocket& sock() const { return sock_; }
  inline TcpSocket& sock() { return sock_; }

  inline TxQueue& tx_queue() { return tx_queue_; }

 private:
  SockAddr GetPeerAddr();

  TcpSocket sock_;
  ioworker::IoWorker<Endpoint>& io_worker_;
  TxQueue tx_queue_;
  Interest interest_;
};
}  // namespace client

class KVStoreTcp final : public KVStore {
 public:
  virtual ~KVStoreTcp() {}

  int Init() override;

  int Put(const std::vector<Key>& keys,
          const std::vector<Value>& values) override;

  int Get(const std::vector<Key>& keys,
          const std::vector<Value*>& values,
          const Callback& cb = nullptr) override;

  int Delete(const std::vector<Key>& keys,
             const Callback& cb = nullptr) override;

  int Register(const char* buf, size_t size) override;

  void Free(Value* value) override;

 private:
  std::vector<std::unique_ptr<ioworker::IoWorker<client::Endpoint>>>
      io_workers_;
};

}  // namespace dialga

#endif  // DIALGA_KVSTORE_TCP_HPP_
