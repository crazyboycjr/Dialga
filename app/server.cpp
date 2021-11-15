#include <memory>
#include <infiniband/verbs.h>
#include <glog/logging.h>

#include "dialga/kvstore.hpp"

int main(int argc, char** argv) {
  ibv_fork_init();
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto server = std::unique_ptr<dialga::KVServer>(dialga::KVServer::Create("rdma"));
  server->Init();
  server->Run();
}
