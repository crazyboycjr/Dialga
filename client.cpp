#include "kvstore.hpp"

int main(int argc, char** argv) {
  ibv_fork_init();
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  kvstore::RdmaKVStore client;
  client.Init();
  char* buffer = client.RdmaMalloc(65536);
  LOG(INFO) << "buffer allocated success. Pointer: " << (uint64_t)buffer;
  client.FakePost(buffer);
  while (true) ;
}