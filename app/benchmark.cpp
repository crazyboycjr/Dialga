#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>
#include <malloc.h>
#include <x86intrin.h>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>

#include "dialga/config.hpp"
#include "dialga/kvstore.hpp"
#include "prism/utils.h"

using prism::Now64;

DEFINE_bool(latency, false, "Run Latency test?");
DEFINE_bool(throughput, false, "Run throughput test?");
DEFINE_bool(test_get, false, "Test with GET operation?");
DEFINE_bool(test_put, false, "Test with PUT operation?");

DEFINE_uint64(total_keys, 1024, "The total number of keys&values.");
DEFINE_uint64(iters, 1000, "Number of iterations.");
DEFINE_uint64(value_len, 128, "The size of each value.");

using namespace dialga;

void PrepareKeyValues(size_t num_keys, size_t value_len,
                      std::vector<Key>* keys,
                      std::vector<ZValue>* values) {
  for (size_t i = 0; i < num_keys; i++) {
    keys->push_back(i);
    auto val = SArray<char>(value_len, 42);
    values->push_back(std::move(val));
  }
}

void InitPut(KVStore* store, const std::vector<Key>& keys,
             const std::vector<ZValue>& values) {
  // PUT prepared key values
  std::atomic<bool> ready{false};
  CHECK(!store->Put(keys, values, [&ready]() { ready.store(true); }));
  while (!ready.load()) _mm_pause();
  // start latency test until initial puts are done
}

int LatencyTest(KVStore* store) {
  size_t num_keys = FLAGS_total_keys;
  size_t iters = FLAGS_iters;
  size_t value_len = FLAGS_value_len;

  std::vector<Key> keys;
  std::vector<ZValue> values;

  PrepareKeyValues(num_keys, value_len, &keys, &values);

  // PUT prepared key values and synchronize
  InitPut(store, keys, values);

  std::vector<uint64_t> latencies;
  for (size_t i = 0; i < iters; i++) {
    Key test_key = (random() % num_keys);
    std::vector<Key> test_keys = {test_key};
    std::vector<ZValue*> test_values { new SArray<char>(value_len) };
    auto before = Now64();
    std::atomic<bool> ready {false};
    CHECK(!store->ZGet(test_keys, &test_values, [&ready](){ ready.store(true); }));
    while (!ready.load()) {
      for (size_t i = 0; i < 16; i++) {
        _mm_pause();
      }
    }
    auto after = Now64();
    latencies.push_back(after - before);

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

int ThroughputTest(KVStore* store) {
  size_t num_keys = FLAGS_total_keys;
  size_t iters = FLAGS_iters;
  size_t value_len = FLAGS_value_len;

  std::vector<Key> keys;
  std::vector<ZValue> values;

  PrepareKeyValues(num_keys, value_len, &keys, &values);

  // PUT prepared key values and synchronize
  InitPut(store, keys, values);

  std::vector<Key> test_keys {0};
  std::vector<std::vector<ZValue*>> test_values_gets;
  std::vector<std::vector<ZValue>> test_values_puts;
  for (size_t i = 0; i < iters; i++) {
    int j = (random() % num_keys);
    auto val = new SArray<char>(values[j].bytes());
    test_values_gets.push_back({val});
    test_values_puts.push_back({values[j]});
  }

  std::atomic<size_t> ready {0};
  auto before = Now64();
  for (size_t i = 0; i < iters; i++) {
    Key test_key = (random() % num_keys);
    test_keys[0] = test_key;
    if (FLAGS_test_get) {
      CHECK(!store->ZGet(test_keys, &test_values_gets[i],
                         [&ready]() { ready.fetch_add(1); }));
    }
    if (FLAGS_test_put) {
      CHECK(!store->Put(test_keys, test_values_puts[i],
                        [&ready]() { ready.fetch_add(1); }));
    }
  }

  // wait until all operations are complete
  while (ready.load() < iters) {
    for (size_t i = 0; i < 16; i++) {
      _mm_pause();
    }
  }

  auto after = Now64();

  if (FLAGS_test_get) {
    for (size_t i = 0; i < iters; i++) {
      delete test_values_gets[i][0];
    }
  }

  LOG(INFO) << "#iters:     " << iters;
  LOG(INFO) << "Duration:   " << after - before << " us";
  LOG(INFO) << "Throughput: " << 1e6 * iters / (after - before) << " op/s";
  LOG(INFO) << "Bandwidth:  " << 1e-3 * iters * value_len * 8 / (after - before) << " Gb/s";
  return 0;
}

int main(int argc, char* argv[]) {
  ibv_fork_init();
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto store = KVStore::Create(FLAGS_comm.c_str());
  CHECK(!store->Init()) << "Failed to initialize KVStore";

  if (!FLAGS_latency && !FLAGS_throughput) {
    LOG(ERROR) << "You should choose either latency test or throughput test";
    return -1;
  }
  if (!FLAGS_test_put && !FLAGS_test_get) {
    LOG(ERROR) << "You should choose either put test or get test";
    return -1;
  }
  if (FLAGS_latency) {
    LatencyTest(store);
  }
  if (FLAGS_throughput) {
    ThroughputTest(store);
  }
  return 0;
}
