#ifndef DIALGA_KVSERVER_TCP_HPP_
#define DIALGA_KVSERVER_TCP_HPP_
#include "dialga/kvstore.hpp"
#include "prism/thread_proto.h"
#include "socket/socket.h"
#include "socket/poll.h"
#include "./io_worker.hpp"

#include <map>
#include <queue>
#include <memory>

namespace dialga {

namespace server {
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
}  // namespace server

class KVServerTcp final : public KVServer {
 public:
  virtual ~KVServerTcp() {}

  int Init() override;

  int Run() override;

  static int ExitHandler(int sig, void* app_ctx);

 private:
  std::vector<std::unique_ptr<ioworker::IoWorker<server::Endpoint>>>
      io_workers_;
};

}  // namespace dialga

#endif  // DIALGA_KVSERVER_TCP_HPP_
