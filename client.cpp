#include <malloc.h>

#include "kvstore-rdma.hpp"

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
  client.Register(buffer, 65536, &lkey, &rkey);
  // Register buffer size should be strictly sizeof(TxMessage) larger than value.size
  kvstore::Value v;
  v.addr_ = (uint64_t) buffer;
  v.size_ = 1024;
  kvstore::Key k;
  k = 1;
  std::vector<kvstore::Key> key_vec;
  key_vec.push_back(k);
  std::vector<kvstore::Value> value_vec;
  value_vec.push_back(v);
  client.Put(key_vec, value_vec);
  client.Put(key_vec, value_vec);
  while (true)
    ;
}