#include <gflags/gflags.h>
#include <malloc.h>

#include <memory>
#include <algorithm>
#include <functional>
#include <iostream>

#include <glog/logging.h>
#include <infiniband/verbs.h>

#include "prism/utils.h"
#include "dialga/kvstore.hpp"
#include "dialga/config.hpp"

using prism::Now64;

volatile bool ready = false;
int value_length = 128;

DEFINE_bool(validation, false, "Run validation test?");
DEFINE_bool(latency, false, "Run Latency test?");

void GetCallBack() { ready = true; }

std::string RandomString() {
  std::string res = "";
  for (int i = 0; i < value_length; i++) {
    res += (char)((random() % 35) + 65);
  }
  return res;
}

int Validation(dialga::KVStore* client, int num_of_key, int testnum) {
  std::vector<char*> buffers;
  std::vector<dialga::Key> keys;
  std::vector<dialga::Value> values;
  LOG(INFO) << "Validation starts";
  for (int i = 0; i < num_of_key; i++) {
    char* buffer = (char*)memalign(sysconf(_SC_PAGESIZE), 4096);
    memset(buffer, 0, value_length);
    std::string val = RandomString();
    strncpy(buffer, val.c_str(), val.size());
    client->Register(buffer, value_length);
    buffers.push_back(buffer);
    dialga::Value v((uint64_t)buffer, value_length);
    values.push_back(v);
    keys.push_back(i);
    LOG(INFO) << "key, value pair generated: " << keys[i] << " " << buffer;
  }
  for (int i = 0; i < num_of_key; i++) {
    std::vector<dialga::Key> put_keys;
    std::vector<dialga::Value> put_values;
    put_keys.push_back(keys[i]);
    put_values.push_back(values[i]);
    client->Put(put_keys, put_values);
  }
  LOG(INFO) << "Client PUT finished. Starting testing....";
  LOG(INFO) << "Full Key testing starts: ";
  for (int i = 0; i < num_of_key; i++) {
    std::vector<dialga::Key> test_keys;
    std::vector<dialga::Value*> test_values;
    int test_key = i;
    test_keys.push_back(i);
    test_values.push_back(new dialga::Value(0, 0));
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
    client->Free(test_values[0]);
    delete test_values[0];
  }
  LOG(INFO) << "Full Key testing over...";
  LOG(INFO) << "Random Key testing starts:";
  for (int i = 0; i < testnum; i++) {
    std::vector<dialga::Key> test_keys;
    std::vector<dialga::Value*> test_values;
    int test_key = (random() % num_of_key);
    test_keys.push_back(test_key);
    test_values.push_back(new dialga::Value(0, 0));
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
      LOG(ERROR) << "The error result is: ";
      for (int j = 0; j < value_length; j++) {
        if (!test_result[j]) break;
        LOG(INFO) << (int)test_result[j];
      }
      break;
    }
    client->Free(test_values[0]);
    delete test_values[0];
  }
  LOG(INFO) << "Random Key testing over...";
  LOG(INFO) << "Testing over....";
  return 0;
}

int LatencyTest(dialga::KVStore* client, int num_of_key, int iters) {
  std::vector<char*> buffers;
  std::vector<dialga::Key> keys;
  std::vector<dialga::Value> values;
  LOG(INFO) << "Validation starts";
  for (int i = 0; i < num_of_key; i++) {
    char* buffer = (char*)memalign(sysconf(_SC_PAGESIZE), 4096);
    memset(buffer, 0, value_length);
    std::string val = RandomString();
    strncpy(buffer, val.c_str(), val.size());
    client->Register(buffer, value_length);
    buffers.push_back(buffer);
    dialga::Value v((uint64_t)buffer, value_length);
    values.push_back(v);
    keys.push_back(i);
    LOG(INFO) << "key, value pair generated: " << keys[i] << " " << buffer;
  }
  for (int i = 0; i < num_of_key; i++) {
    std::vector<dialga::Key> put_keys;
    std::vector<dialga::Value> put_values;
    put_keys.push_back(keys[i]);
    put_values.push_back(values[i]);
    client->Put(put_keys, put_values);
  }
  LOG(INFO) << "Client PUT finished. Starting Latency testing (we only test "
               "GET currently)";
  std::vector<uint64_t> latencies;
  for (int i = 0; i < iters; i++) {
    std::vector<dialga::Key> test_keys;
    std::vector<dialga::Value*> test_values;
    int test_key = (random() % num_of_key);
    test_keys.push_back(test_key);
    test_values.push_back(new dialga::Value(0, 0));
    auto before = Now64();
    client->Get(test_keys, test_values, GetCallBack);
    while (!ready)
      ;
    auto after = Now64();
    char* test_result = (char*)test_values[0]->addr_;
    if (strncmp(test_result, buffers[test_key], value_length) == 0) {
      // LOG(INFO) << "Test " << i << " (key is " << test_key
                // << ") success, latency is " << after - before;
      latencies.push_back(after - before);
      ready = false;
    } else {
      LOG(ERROR) << "Test " << i << " failed. Key is " << test_key
                 << "; Read value is " << test_result << " , actual value is "
                 << buffers[test_key];
      break;
    }
    client->Free(test_values[0]);
    delete test_values[0];
  }
  std::sort(latencies.begin(), latencies.end());
  LOG(INFO) << "Min:    " << latencies[0];
  LOG(INFO) << "Median: " << latencies[latencies.size() / 2];
  LOG(INFO) << "P95:    " << latencies[int(latencies.size() * 0.95)];
  LOG(INFO) << "P99:    " << latencies[int(latencies.size() * 0.99)];
  LOG(INFO) << "Max:    " << latencies[latencies.size() - 1];
  LOG(INFO) << "Testing over....";
  return 0;
}

int main(int argc, char** argv) {
  ibv_fork_init();
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto client = dialga::KVStore::Create(dialga::FLAGS_comm.c_str());
  client->Init();
  if (FLAGS_validation) {
    int batch = 1, iters = 1;
    LOG(INFO) << "Please input the validation parameters: ";
    LOG(INFO) << "Please input batch: how many keys there are?";
    std::cin >> batch;
    LOG(INFO) << "Please input test iters: how many tests you want to do for "
                 "validation?";
    std::cin >> iters;
    Validation(client, batch, iters);
  }
  if (FLAGS_latency) {
    int batch = 1, iters = 1;
    LOG(INFO) << "Please input the validaiton parameters: ";
    LOG(INFO) << "Please input batch: how many keys there are?";
    std::cin >> batch;
    LOG(INFO) << "Please input test iters: how many tests you want to do for "
                 "validation?";
    std::cin >> iters;
    LatencyTest(client, batch, iters);
  }
  return 0;
}
