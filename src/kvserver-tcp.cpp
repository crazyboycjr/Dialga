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
  sock_.SetNonBlock(true);
  sock_.SetNodelay(true);
  io_worker_.poll().registry().Register(
      sock_, Token(reinterpret_cast<uintptr_t>(this)), interest_);
  PrepareNewReceive();
  tx_stage_ = TxStage::NothingToSend;
  LOG(INFO) << "Accept connection from: " << GetPeerAddr().AddrStr();
}

SockAddr Endpoint::GetPeerAddr() {
  struct sockaddr_storage storage;
  struct sockaddr* sockaddr = reinterpret_cast<struct sockaddr*>(&storage);
  socklen_t len = sizeof(struct sockaddr_storage);
  PCHECK(!getpeername(sock_, sockaddr, &len));
  return SockAddr(sockaddr, len);
}

KVServerTcp* Endpoint::GetKVServer() {
  return io_worker_.context<KVServerTcp>();
}

void Endpoint::NotifyWork() {
  if (!interest_.IsWritable()) {
    // add this endpoint back to epoll set
    interest_ = interest_.Add(Interest::WRITABLE);
    io_worker_.poll().registry().Reregister(
        sock_, Token(reinterpret_cast<uintptr_t>(this)), interest_);
  }
}

bool Endpoint::Overloaded() {
  return tx_queue().size() >= 1024;
}

void Endpoint::OnSendReady() {
  // Send until there's nothing in the TxQueue
  while (true) {
    if (tx_stage_ == TxStage::NothingToSend) {
      if (tx_queue_.empty()) {
        if (interest_.IsWritable()) {
          // remove this endpoint from the writable interested set, saving CPU,
          // but will increase latency.
          interest_ = interest_.Remove(Interest::WRITABLE);
          io_worker_.poll().registry().Reregister(
              sock_, Token(reinterpret_cast<uintptr_t>(this)), interest_);
        }
        return;
      }

      Message& msg = tx_queue_.front();
      PrepareNewSend(msg);
      tx_queue_.pop();
    }

    // to this point, there must be something ready to send in the tx_buffer_
    ssize_t nbytes = sock_.Send(tx_buffer_.Chunk(), tx_buffer_.Remaining());

    if (nbytes == -1) {
      return;
    }

    tx_buffer_.Advance(nbytes);

    if (!tx_buffer_.HasRemaining()) {
      switch (tx_stage_) {
        case TxStage::Meta: {
          tx_buffer_ = Buffer(tx_kvs_.keys.data(), tx_kvs_.keys.bytes(), tx_kvs_.keys.bytes());
          tx_stage_ = TxStage::Keys;
          break;
        }
        case TxStage::Keys: {
          tx_buffer_ = Buffer(tx_kvs_.lens.data(), tx_kvs_.lens.bytes(), tx_kvs_.lens.bytes());
          tx_stage_ = TxStage::Lens;
          break;
        }
        case TxStage::Lens: {
          tx_value_index_ = 0;
          tx_buffer_ = Buffer(tx_kvs_.values[tx_value_index_]->data(),
                              tx_kvs_.values[tx_value_index_]->bytes(),
                              tx_kvs_.values[tx_value_index_]->bytes());
          tx_stage_ = TxStage::Values;
          break;
        }
        case TxStage::Values: {
          tx_value_index_++;
          if (tx_value_index_ < tx_meta_.num_keys) {
            // move the buffer to the next value
            tx_buffer_ = Buffer(tx_kvs_.values[tx_value_index_]->data(),
                                tx_kvs_.values[tx_value_index_]->bytes(),
                                tx_kvs_.values[tx_value_index_]->bytes());
          } else {
            // change tx_stage to NothingToSend
            tx_stage_ = TxStage::NothingToSend;
          }
          break;
        }
        default: {
          LOG(FATAL) << "Unexpected state: " << static_cast<int>(tx_stage_);
          break;
        }
      }
    }
  }
}

void Endpoint::OnRecvReady() {
  while (true) {
    if (Overloaded()) {
      return;
    }

    ssize_t nbytes = sock_.Recv(rx_buffer_.Chunk(), rx_buffer_.Remaining());

    if (nbytes <= 0) {
      return;
    }

    rx_buffer_.Advance(nbytes);

    if (!rx_buffer_.HasRemaining()) {
      switch (rx_stage_) {
        case RxStage::Meta: {
          // allocate KVPairs, values are not allocated until lens are received
          kvs_ = KVPairs(meta_);
          // update the buffer to points to the keys, then lens, then values
          // accordingly
          rx_buffer_ = Buffer(kvs_.keys.data(), kvs_.keys.bytes(), kvs_.keys.bytes());
          // move to the next stage
          rx_stage_ = RxStage::Keys;

          break;
        }
        case RxStage::Keys: {
          // update the buffer to lens
          // move to the next stage depending on the operation
          if (meta_.op == Operation::PUT) {
            rx_buffer_ = Buffer(kvs_.lens.data(), kvs_.lens.bytes(), kvs_.lens.bytes());
            rx_stage_ = RxStage::Lens;
          } else if (meta_.op == Operation::GET || meta_.op == Operation::DELETE) {
            // kvpair is finished receiving, pass to the backend storage
            Message msg = {meta_.op, meta_.timestamp, this, kvs_};
            GetKVServer()->ProcessMsg(msg, this);
            // start over for new request
            PrepareNewReceive();
          }

          break;
        }
        case RxStage::Lens: {
          // allocate space for values
          kvs_.values.resize(meta_.num_keys);
          uint32_t i = 0;
          for (std::lock_guard<std::mutex> lk(GetKVServer()->mu_);
               i < meta_.num_keys; i++) {
            auto it = GetKVServer()->storage_.find(kvs_.keys[i]);
            if (it != GetKVServer()->storage_.end()) {
              // if the key already exists, and the space is enough, put it in
              // the existing space
              kvs_.values[i] = it->second;
              kvs_.values[i]->resize(kvs_.lens[i]);
            } else {
              // kvs_.values[i] = new SArray<char>(kvs_.lens[i] / sizeof(char));
              // bypass the memset for performance
              char* ptr = SArray<char>::allocator::Alloc(kvs_.lens[i]);
              kvs_.values[i] = new SArray<char>(ptr, kvs_.lens[i], true);
            }
          }

          rx_value_index_ = 0;
          // update the buffer to the first value
          rx_buffer_ = Buffer(kvs_.values[rx_value_index_]->data(),
                              kvs_.values[rx_value_index_]->bytes(),
                              kvs_.values[rx_value_index_]->bytes());
          // move to the next stage
          rx_stage_ = RxStage::Values;
          break;
        }
        case RxStage::Values: {
          rx_value_index_++;
          CHECK_EQ(meta_.op, Operation::PUT);
          if (rx_value_index_ < meta_.num_keys) {
            // move the buffer to the next value
            rx_buffer_ = Buffer(kvs_.values[rx_value_index_]->data(),
                                kvs_.values[rx_value_index_]->bytes(),
                                kvs_.values[rx_value_index_]->bytes());
          } else {
            // if the kvpair is finished, pass the entire message to the backend
            // storage
            Message msg = {meta_.op, meta_.timestamp, this, kvs_};
            GetKVServer()->ProcessMsg(msg, this);
            // start over again
            PrepareNewReceive();
          }
          break;
        }
        default: {
          // For C++, you always have to check this... (Thank you C++!)
          LOG(FATAL) << "Unexpected state: " << static_cast<int>(rx_stage_);
        }
      }
    }
  }
}

void Endpoint::PrepareNewReceive() {
  // This will be called after connection setup and the end of receiving an
  // entire request.
  rx_stage_ = RxStage::Meta;
  rx_buffer_ = Buffer(&meta_, sizeof(meta_), sizeof(meta_));
}

void Endpoint::PrepareNewSend(const Message& msg) {
  tx_stage_ = TxStage::Meta;
  tx_meta_.op = msg.op;
  tx_meta_.num_keys = msg.kvs.keys.size();
  tx_meta_.timestamp = msg.timestamp;
  tx_kvs_ = msg.kvs;
  tx_buffer_ = Buffer(&tx_meta_, sizeof(tx_meta_), sizeof(tx_meta_));
}

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
    // Use SO_REUSEPORT to allow multiple epoll instances to share one port
    listener.SetReusePort(true);
    listener.SetNonBlock(true);
    listener.Bind(ai);
    listener.Listen();

    LOG(INFO) << "Socket server is listening on uri: " << ai.AddrStr();

    auto io_worker = std::make_unique<IoWorker<server::Endpoint>>(this, listener);
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

using namespace server;

void KVServerTcp::ProcessMsg(Message& msg, Endpoint* endpoint) {
  // This function may have many concurrent callers
  switch (msg.op) {
    case Operation::PUT: {
      auto& kvs = msg.kvs;
      CHECK(!kvs.keys.empty());
      CHECK_EQ(kvs.keys.size(), kvs.values.size());
      std::lock_guard<std::mutex> lk(mu_);
      for (size_t i = 0; i < kvs.keys.size(); i++) {
        auto key = kvs.keys[i];
        storage_[key] = kvs.values[i];
      }
      // PUT does not send response back
      break;
    }
    case Operation::GET: {
      auto& kvs = msg.kvs;
      CHECK(!kvs.keys.empty());
      CHECK(kvs.lens.empty());
      CHECK(kvs.values.empty());
      auto num_keys = kvs.keys.size();
      kvs.lens.reserve(num_keys);
      kvs.values.reserve(num_keys);
      size_t i = 0;
      for (std::lock_guard<std::mutex> lk(mu_); i < num_keys; i++) {
        auto key = kvs.keys[i];
        auto value = storage_[key];
        kvs.lens.push_back(value->bytes());
        kvs.values.push_back(value);
      }
      // GET sends back the msg to the tx_queue of the endpoint
      endpoint->tx_queue().push(std::move(msg));
      endpoint->NotifyWork();
      endpoint->OnSendReady();
      break;
    }
    case Operation::DELETE: {
      LOG(WARNING) << "DELETE hasn't been implemented.";
      break;
    }
    default: {
      LOG(WARNING) << "Unrecognized operation: " << msg.op << " from "
                   << endpoint->GetPeerAddr().AddrStr();
    }
  }
}

}  // namespace dialga
