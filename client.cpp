#include <malloc.h>

#include <iostream>

#include "kvstore-rdma.hpp"
#include "kvstore.hpp"

bool ready = false;

void GetCallBack() {
  LOG(INFO) << "Get is finished. The callback is triggered.";
  ready = true;
}

std::string RandomString() {
  std::string res = "";
  for (int i = 0; i < 32; i++) {
    res += (char)((random() % 35) + 65);
  }
  return res;
}

int main(int argc, char** argv) {
  ibv_fork_init();
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  kvstore::RdmaKVStore client;
  client.Init();
  std::vector<kvstore::Key> key_vec;
  std::vector<char*> buffers;
  std::vector<kvstore::Value> values;
  for (int i = 0; i < 32; i++) {
    char* buffer = (char*)memalign(sysconf(_SC_PAGESIZE), 65536);
    memset(buffer, 0, 65536);
    std::string val = RandomString();
    strncpy(buffer, val.c_str(), val.size());
    client.Register(buffer, 65536);
    buffers.push_back(buffer);
    kvstore::Value v;
    v.addr_ = (uint64_t)buffer;
    v.size_ = 61920;
    values.push_back(v);
    key_vec.push_back(i + 200);
    LOG(INFO) << "key is " << key_vec[i] << " , value is " << (char*)buffers[i];
  }
  // Register buffer size should be strictly sizeof(TxMessage) larger than
  // value.size
  // while (true) {
  //   std::vector<kvstore::Key> key_vec;
  //   std::vector<kvstore::Value> value_vec;
  //   std::cin >> k;
  //   key_vec.push_back(k);
  //   value_vec.push_back(v);
  //   client.Put(key_vec, value_vec);
  //
  std::vector<kvstore::Value*> output_value_vec;
  for (int i = 0; i < 32; i++) {
    output_value_vec.push_back(new kvstore::Value);
  }
  client.Put(key_vec, values);
  LOG(INFO) << "Client.Put() finished";
  client.Get(key_vec, output_value_vec, GetCallBack);
  while (!ready)
    ;
  for (int i = 0; i < output_value_vec.size(); i++) {
    char* res = (char*)output_value_vec[i]->addr_;
    LOG(INFO) << "Client.Get() finished, key is " << key_vec[i]
              << " , result is " << res;
  }
}