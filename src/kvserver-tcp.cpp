#include "./kvserver-tcp.hpp"

#include <signal.h>

#include <memory>
#include <string>
#include <vector>

#include "./io_worker.hpp"
#include "dialga/config.hpp"
#include "glog/logging.h"

namespace dialga {

using namespace ioworker;
using namespace socket;

namespace server {

Endpoint::Endpoint(TcpSocket sock, IoWorker<Endpoint>& io_worker)
    : sock_{sock},
      io_worker_{io_worker},
      interest_{Interest::READABLE | Interest::WRITABLE} {}

void Endpoint::OnError() {
  LOG(ERROR) << "Socket error: " << sock_.GetSockError()
             << ", remote uri: " << GetPeerAddr().AddrStr();
  io_worker_.poll().registry().Deregister(sock_);
  sock_.Close();
}

void Endpoint::OnEstablished() {
  GetPeerAddr();
  sock_.SetNonBlock(true);
  io_worker_.poll().registry().Register(
      sock_, Token(reinterpret_cast<uintptr_t>(this)), interest_);
  LOG(INFO) << "Accept connection from: " << GetPeerAddr().AddrStr();
}

SockAddr Endpoint::GetPeerAddr() {
  struct sockaddr_storage storage;
  struct sockaddr* sockaddr = reinterpret_cast<struct sockaddr*>(&storage);
  socklen_t len = sizeof(struct sockaddr_storage);
  PCHECK(!getpeername(sock_, sockaddr, &len));
  return SockAddr(sockaddr, len);
}

void Endpoint::OnSendReady() {}

void Endpoint::OnRecvReady() {}
}  // namespace server

/// KVServer Implementation
int KVServerTcp::Init() {
  // Create a socket that bind and listen on the port.
  AddrInfo ai(FLAGS_port, SOCK_STREAM);

  // Pass the listener to a bunch of IoWorkers.
  for (uint32_t i = 0; i < FLAGS_num_io_workers; i++) {
    TcpSocket listener;
    listener.Create(ai);
    listener.SetReuseAddr(true);
    // Use SO_REUSEPORT to allow multiple epoll instance sharing one port
    listener.SetReusePort(true);
    listener.SetNonBlock(true);
    listener.Bind(ai);
    listener.Listen();

    LOG(INFO) << "Socket server is listening on uri: " << ai.AddrStr();

    auto io_worker = std::make_unique<IoWorker<server::Endpoint>>(listener);
    io_workers_.push_back(std::move(io_worker));
  }

  return 0;
}

int KVServerTcp::ExitHandler(int sig, void* app_ctx) {
  // install a exit handler
  static KVServerTcp* inst = nullptr;
  if (!inst) {
    inst = static_cast<KVServerTcp*>(app_ctx);
    return 0;
  }
  LOG(INFO) << "signal " << sig << " received, exiting...";
  for (auto& io_worker : inst->io_workers_) {
    io_worker->Terminate();
  }
  return 0;
}

int KVServerTcp::Run() {
  // Register a Ctrl-C signal handler.
  ExitHandler(0, this);
  signal(SIGINT, (void (*)(int))KVServerTcp::ExitHandler);

  // Start all the threads. I really don't know now why this have to be a
  // seperate operation in C++.
  for (auto& io_worker : io_workers_) {
    io_worker->Start();
  }

  // Wait for all the IoWorkers to finish.
  for (auto& io_worker : io_workers_) {
    io_worker->Join();
  }
  return 0;
}

}  // namespace dialga
