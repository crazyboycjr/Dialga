#include <malloc.h>

#include <iostream>

#include "kvstore-rdma.hpp"

void PutCallBack() {
  LOG(INFO) << "PUT is finished. Tha callback is triggered.";
}

int main(int argc, char** argv) {
  ibv_fork_init();
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  kvstore::RdmaKVStore client;
  client.Init();
  char* buffer = (char*)memalign(sysconf(_SC_PAGESIZE), 65536);
  memset(buffer, 0, 65536);
  LOG(INFO) << "buffer allocated success. Pointer: " << (uint64_t)buffer;
  strncpy(buffer, "1a2b3c4d5e6f", sizeof("1a2b3c4d5e6f"));
  uint32_t lkey, rkey;
  client.Register(buffer, 65536);
  // Register buffer size should be strictly sizeof(TxMessage) larger than
  // value.size
  kvstore::Value v;
  v.addr_ = (uint64_t)buffer;
  v.size_ = 61920;
  kvstore::Key k;
  k = 192;
  // while (true) {
  //   std::vector<kvstore::Key> key_vec;
  //   std::vector<kvstore::Value> value_vec;
  //   std::cin >> k;
  //   key_vec.push_back(k);
  //   value_vec.push_back(v);
  //   client.Put(key_vec, value_vec);
  // }
  while (true) {
    std::vector<kvstore::Key> key_vec;
    std::vector<kvstore::Value> value_vec;
    for (int i = 0; i < 32; i++) {
      key_vec.push_back(k);
      value_vec.push_back(v);
    }
    client.Put(key_vec, value_vec);
  }
}