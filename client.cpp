#include <gflags/gflags.h>
#include <malloc.h>

#include <iostream>

#include "kvstore-rdma.hpp"
#include "kvstore.hpp"

bool ready = false;
int value_length = 128;

DEFINE_bool(validation, false, "Run validation test?");

void GetCallBack() {
  ready = true;
}

std::string RandomString() {
  std::string res = "";
  for (int i = 0; i < value_length; i++) {
    res += (char)((random() % 35) + 65);
  }
  return res;
}

int Validation(kvstore::RdmaKVStore* client, int num_of_key, int testnum) {
  std::vector<char*> buffers;
  std::vector<kvstore::Key> keys;
  std::vector<kvstore::Value> values;
  LOG(INFO) << "Validation starts";
  for (int i = 0; i < num_of_key; i++) {
    char* buffer = (char*)memalign(sysconf(_SC_PAGESIZE), 4096);
    memset(buffer, 0, value_length);
    std::string val = RandomString();
    strncpy(buffer, val.c_str(), val.size());
    client->Register(buffer, value_length);
    buffers.push_back(buffer);
    kvstore::Value v;
    v.addr_ = (uint64_t)buffer;
    v.size_ = value_length;
    values.push_back(v);
    keys.push_back(i);
    LOG(INFO) << "key, value pair generated: " << keys[i] << " " << buffer;
  }
  for (int i = 0; i < num_of_key; i++) {
    std::vector<kvstore::Key> put_keys;
    std::vector<kvstore::Value> put_values;
    put_keys.push_back(keys[i]);
    put_values.push_back(values[i]);
    client->Put(put_keys, put_values);
  }
  LOG(INFO) << "Client PUT finished. Starting testing....";
  for (int i = 0; i < testnum; i++) {
    std::vector<kvstore::Key> test_keys;
    std::vector<kvstore::Value*> test_values;
    int test_key = (random() % num_of_key);
    test_keys.push_back(test_key);
    test_values.push_back(new kvstore::Value);
    client->Get(test_keys, test_values, GetCallBack);
    while (!ready)
      ;
    char* test_result = (char*)test_values[0]->addr_;
    if (strncmp(test_result, buffers[test_key], value_length) == 0) {
      LOG(INFO) << "Test " << i << " (key = " << test_key << ") passed...";
      ready = false;
    } else {
      LOG(ERROR) << "Test " << i << " failed. Key is " << test_key
                 << "; Read value is " << test_result << " , actual value is "
                 << buffers[test_key];
      break;
    }
  }
  LOG(INFO) << "Testing over....";
  return 0;
}

int main(int argc, char** argv) {
  ibv_fork_init();
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  kvstore::RdmaKVStore* client = new kvstore::RdmaKVStore;
  client->Init();
  if (FLAGS_validation) {
    int batch = 1, iters = 1;
    LOG(INFO) << "Please input batch: how many keys there are?";
    std::cin >> batch;
    LOG(INFO) << "Please input test iters: how many tests you want to do for "
                 "validation?";
    std::cin >> iters;

    Validation(client, batch, iters);
  }
  return 0;
}