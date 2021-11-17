#include "./kvstore-tcp.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "dialga/config.hpp"

namespace dialga {

using namespace ioworker;
using namespace socket;

namespace client {

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
  LOG(INFO) << "Connected to: " << GetPeerAddr().AddrStr();
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
}  // namespace client

int KVStoreTcp::Init() {
  auto hostvec = GetHostList(FLAGS_connect);
  for (const auto& host : hostvec) {
    std::string hostname;
    int port;
    ParseHostPort(host, hostname, port);
    TcpSocket sock;
    AddrInfo ai(hostname.c_str(), port, SOCK_STREAM);
    LOG(INFO) << "Socket client is connecting to remote_uri: " << ai.AddrStr();
    sock.Create(ai);
    while (!sock.Connect(ai)) {
      LOG(WARNING) << "Connect failed, error code: " << sock.GetLastError()
                   << ", retrying after 1 second...";
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    // for each active connection, set a dedicate IoWorker for it
    auto io_worker = std::make_unique<IoWorker<client::Endpoint>>(this);
    io_worker->AddNewConnection(sock);
    io_workers_.push_back(std::move(io_worker));
  }

  return 0;
}

int KVStoreTcp::Put(const std::vector<Key>& keys,
                    const std::vector<Value>& values) {
  LOG(FATAL) << "unimplemented";
}

int KVStoreTcp::Get(const std::vector<Key>& keys,
                    const std::vector<Value*>& values, const Callback& cb) {
  LOG(FATAL) << "unimplemented";
}

int KVStoreTcp::Delete(const std::vector<Key>& keys, const Callback& cb) {
  LOG(FATAL) << "unimplemented";
}

int KVStoreTcp::Register(const char* buf, size_t size) {
  LOG(FATAL) << "unimplemented";
}

void KVStoreTcp::Free(Value* value) { LOG(FATAL) << "unimplemented"; }

}  // namespace dialga
