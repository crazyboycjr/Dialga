#ifndef DIALGA_IO_WORKER_HPP_
#define DIALGA_IO_WORKER_HPP_
#include <map>
#include <memory>
#include <queue>

#include "dialga/kvstore.hpp"
#include "prism/thread_proto.h"
#include "socket/poll.h"
#include "socket/socket.h"

namespace dialga {
namespace ioworker {

using namespace socket;

template <typename Endpoint>
class IoWorker : public TerminableThread {
 public:
  explicit IoWorker(void* context)
      : context_{context},
        listener_{INVALID_SOCKET},
        poll_{socket::Poll::Create()} {}

  explicit IoWorker(void* context, socket::TcpSocket listener)
      : context_{context}, listener_{listener}, poll_{socket::Poll::Create()} {
    poll_.registry().Register(listener_, Token(listener_.sockfd),
                              Interest::READABLE);
  }

  inline socket::Poll& poll() { return poll_; }

  template <typename Context>
  inline Context& context() {
    return *static_cast<Context*>(context_);
  }

  template <typename Context>
  inline const Context& context() const {
    return *static_cast<const Context*>(context_);
  }

  inline const size_t GetNumEndpoints() const { return endpoints_.size(); }

  void Run() override {
    int timeout_ms =
        prism::GetEnvOrDefault<int>("DIALGA_EPOLL_TIMEOUT_MS", 1000);
    int max_events =
        prism::GetEnvOrDefault<int>("DIALGA_EPOLL_MAX_EVENTS", 1024);
    // never resize this vector
    std::vector<Event> events(max_events);
    auto timeout = std::chrono::milliseconds(timeout_ms);

    while (!terminated_.load()) {
      // Epoll IO
      int nevents = poll_.PollUntil(&events[0], max_events, timeout);
      PCHECK(nevents >= 0) << "PollUntil";

      for (int i = 0; i < nevents; i++) {
        auto& ev = events[i];
        if (ev.token().token == static_cast<size_t>(listener_.sockfd)) {
          CHECK(ev.IsReadable());
          AcceptNewConnection();
          continue;
        }

        // data events
        Endpoint* endpoint =
            reinterpret_cast<Endpoint*>(static_cast<uintptr_t>(ev.token()));

        if (ev.IsReadable()) {
          // static auto t1 = std::chrono::high_resolution_clock::now();
          endpoint->OnRecvReady();
          // auto t2 = std::chrono::high_resolution_clock::now();
          // LOG(INFO) << (t2 - t1).count() / 1e3 << " us";
          // t1 = t2;
        }

        if (ev.IsWritable()) {
          endpoint->OnSendReady();
        }

        if (ev.IsError() || ev.IsReadClosed() || ev.IsWriteClosed()) {
          endpoint->OnError();
          endpoints_.erase(endpoint->fd());
        }
      }
    }
  }

  std::shared_ptr<Endpoint> AddNewConnection(TcpSocket new_sock) {
    auto endpoint = std::make_shared<Endpoint>(new_sock, *this);
    endpoint->OnEstablished();
    endpoints_[endpoint->fd()] = endpoint;
    return endpoint;
  }

 private:
  void AcceptNewConnection() {
    TcpSocket new_sock = listener_.Accept();
    AddNewConnection(new_sock);
  }

  // type earsed context.
  void* context_;
  socket::TcpSocket listener_;
  socket::Poll poll_;
  std::map<socket::RawFd, std::shared_ptr<Endpoint>> endpoints_;
};

}  // namespace ioworker
}  // namespace dialga

#endif  // DIALGA_IO_WORKER_HPP_
