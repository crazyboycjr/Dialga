#include "./kvstore-tcp.hpp"
#include "./wire.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "dialga/config.hpp"
#include "dialga/sarray.hpp"

namespace dialga {

using namespace ioworker;
using namespace socket;

namespace client {

Endpoint::Endpoint(TcpSocket sock, IoWorker<Endpoint>& io_worker)
    : sock_{sock},
      io_worker_{io_worker},
      interest_{Interest::READABLE | Interest::WRITABLE} {}

Endpoint::~Endpoint() {
  io_worker_.poll().registry().Deregister(sock_);
  sock_.Close();
}

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
  LOG(INFO) << "Connected to: " << GetPeerAddr().AddrStr();
}

SockAddr Endpoint::GetPeerAddr() {
  struct sockaddr_storage storage;
  struct sockaddr* sockaddr = reinterpret_cast<struct sockaddr*>(&storage);
  socklen_t len = sizeof(struct sockaddr_storage);
  PCHECK(!getpeername(sock_, sockaddr, &len));
  return SockAddr(sockaddr, len);
}

void Endpoint::OnSendReady() {
  // Send until there's nothing in the TxQueue
  while (true) {
    if (tx_stage_ == TxStage::NothingToSend) {
      Message msg;
      if (!tx_queue_.TryPop(&msg)) {
        // unlike the kvserver, we do not disable notification here, so the
        // message will be send out as soon as possible
        return;
      }

      AddCallback(msg);
      PrepareNewSend(msg);
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
          if (tx_meta_.op == Operation::PUT) {
            tx_buffer_ = Buffer(tx_kvs_.lens.data(), tx_kvs_.lens.bytes(), tx_kvs_.lens.bytes());
            tx_stage_ = TxStage::Lens;
          } else if (tx_meta_.op == Operation::GET || tx_meta_.op == Operation::DELETE) {
            tx_stage_ = TxStage::NothingToSend;
          }
          break;
        }
        case TxStage::Lens: {
          tx_value_index_ = 0;
          tx_buffer_ = Buffer(tx_kvs_.values[tx_value_index_].data(),
                              tx_kvs_.values[tx_value_index_].bytes(),
                              tx_kvs_.values[tx_value_index_].bytes());
          tx_stage_ = TxStage::Values;
          break;
        }
        case TxStage::Values: {
          tx_value_index_++;
          CHECK_EQ(tx_meta_.op, Operation::PUT);
          if (tx_value_index_ < tx_meta_.num_keys) {
            // move the buffer to the next value
            tx_buffer_ = Buffer(tx_kvs_.values[tx_value_index_].data(),
                                tx_kvs_.values[tx_value_index_].bytes(),
                                tx_kvs_.values[tx_value_index_].bytes());
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
    ssize_t nbytes = sock_.Recv(rx_buffer_.Chunk(), rx_buffer_.Remaining());

    if (nbytes <= 0) {
      return;
    }

    rx_buffer_.Advance(nbytes);

    if (!rx_buffer_.HasRemaining()) {
      switch (rx_stage_) {
        case RxStage::Meta: {
          CHECK_EQ(meta_.op, Operation::GET);
          kvs_.keys = SArray<Key>(meta_.num_keys);
          kvs_.lens = SArray<uint32_t>(meta_.num_keys);
          kvs_.values.clear();
          rx_buffer_ = Buffer(kvs_.keys.data(), kvs_.keys.bytes(), kvs_.keys.bytes());
          rx_stage_ = RxStage::Keys;

          break;
        }
        case RxStage::Keys: {
          // update the buffer to lens
          CHECK_EQ(meta_.op, Operation::GET);
          rx_buffer_ = Buffer(kvs_.lens.data(), kvs_.lens.bytes(), kvs_.lens.bytes());
          rx_stage_ = RxStage::Lens;

          break;
        }
        case RxStage::Lens: {
          // Now we have the lengths, allocate room for the values
          CHECK_EQ(meta_.op, Operation::GET);
          // For GET operation, the lens and values are left empty, so set them here
          auto it = ctx_table_.find(meta_.timestamp);
          if (it == ctx_table_.end()) {
            LOG(FATAL) << "State not found, timestamp: " << meta_.timestamp;
          }

          CHECK_EQ(it->second.values.size(), meta_.num_keys);
          CHECK(kvs_.values.empty());
          for (size_t i = 0; i < meta_.num_keys; i++) {
            // lens[i] are in bytes
            size_t len = kvs_.lens[i];
            char* addr = static_cast<char*>(malloc(len));
            it->second.values[i]->addr_ = reinterpret_cast<uintptr_t>(addr);
            it->second.values[i]->size_ = kvs_.lens[i];
            auto sarray = SArray<char>(addr, len, false);
            kvs_.values.push_back(sarray);
          }

          rx_value_index_ = 0;
          // update the buffer to the first value
          rx_buffer_ = Buffer(kvs_.values[rx_value_index_].data(),
                              kvs_.values[rx_value_index_].bytes(),
                              kvs_.values[rx_value_index_].bytes());
          // move to the next stage
          rx_stage_ = RxStage::Values;
          break;
        }
        case RxStage::Values: {
          rx_value_index_++;
          CHECK_EQ(meta_.op, Operation::GET);
          if (rx_value_index_ < meta_.num_keys) {
            // move the buffer to the next value
            rx_buffer_ = Buffer(kvs_.values[rx_value_index_].data(),
                                kvs_.values[rx_value_index_].bytes(),
                                kvs_.values[rx_value_index_].bytes());
          } else {
            // if the kvpair is finished, pass the entire message to the backend
            // storage
            RunCallback(meta_.timestamp);
            // start over again
            PrepareNewReceive();
          }
          break;
        }
        default: {
          LOG(FATAL) << "Unexpected state: " << static_cast<int>(rx_stage_);
        }
      }
    }
  }
}

void Endpoint::PrepareNewReceive() {
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

void Endpoint::AddCallback(const Message& msg) {
  ctx_table_[msg.timestamp] = {msg.ctx, msg.values};
}

void Endpoint::RunCallback(uint64_t timestamp) {
  auto it = ctx_table_.find(timestamp);
  if (it == ctx_table_.end()) {
    LOG(FATAL) << "state not found, timestamp: " << timestamp;
  }

  auto ctx = it->second.ctx;
  if (ctx && ctx->UpdateReceived()) {
    if (ctx->cb) ctx->cb();
  }
}

}  // namespace client

int KVStoreTcp::Init() {
  // Each KVStore instance sets up connection to all its servers
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
    auto io_worker = client::IoManager::Get()->ScheduleIoWorker(
        client::ScheduleMode::Dedicate);
    auto endpoint = io_worker->AddNewConnection(sock);
    endpoints_.push_back(endpoint);
  }

  return 0;
}

int KVStoreTcp::Put(const std::vector<Key>& keys,
                    const std::vector<Value>& values) {
  if (keys.size() != values.size()) {
    LOG(ERROR) << "Put() failed due to size mismatch (key & value)";
    return -1;
  }
  CHECK(!endpoints_.empty());

  // slicing
  std::vector<KVPairs> sliced;
  sliced.resize(endpoints_.size());
  for (size_t i = 0; i < keys.size(); i++) {
    auto key = keys[i];
    auto sarray = SArray<char>(reinterpret_cast<char*>(values[i].addr_),
                               values[i].size_, false);
    size_t server_id = key % endpoints_.size();
    sliced[server_id].keys.push_back(key);
    sliced[server_id].lens.push_back(sarray.bytes());
    sliced[server_id].values.push_back(sarray);
  }

  client::Message msg;
  msg.op = Operation::PUT;
  msg.timestamp = timestamp_++;
  msg.ctx = nullptr;
  for (size_t i = 0; i < endpoints_.size(); i++) {
    if (sliced[i].keys.empty()) continue;
    msg.kvs = sliced[i];
    endpoints_[i]->tx_queue().Push(msg);
  }

  return 0;
}

int KVStoreTcp::Get(const std::vector<Key>& keys,
                    std::vector<Value*>& values, const Callback& cb) {
  if (keys.size() != values.size()) {
    LOG(ERROR) << "Get() failed due to size mismatch (key & value)";
    return -1;
  }
  CHECK(!endpoints_.empty());

  // slicing
  std::vector<KVPairs> sliced;
  sliced.resize(endpoints_.size());
  for (size_t i = 0; i < keys.size(); i++) {
    auto key = keys[i];
    size_t server_id = key % endpoints_.size();
    sliced[server_id].keys.push_back(key);
  }

  uint32_t expected = 0;
  for (size_t i = 0; i < endpoints_.size(); i++) {
    expected += sliced[i].keys.empty() ? 0 : 1;
  }

  client::Message msg;
  msg.op = Operation::GET;
  msg.timestamp = timestamp_++;
  msg.ctx = new client::OpContext(expected, cb);
  msg.values = values;
  for (size_t i = 0; i < endpoints_.size(); i++) {
    if (sliced[i].keys.empty()) continue;
    msg.kvs = sliced[i];
    endpoints_[i]->tx_queue().Push(msg);
  }

  return 0;
}

int KVStoreTcp::Delete(const std::vector<Key>& keys, const Callback& cb) {
  LOG(FATAL) << "unimplemented";
}

int KVStoreTcp::Register(const char* buf, size_t size) {
  // TCP register/free does nothing.
  return 0;
}

void KVStoreTcp::Free(Value* value) {
  // TCP register/free does nothing.
}

}  // namespace dialga
