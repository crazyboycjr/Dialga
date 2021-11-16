#include "dialga/config.hpp"

#include <gflags/gflags.h>

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace dialga {

DEFINE_string(connect, "",
              "The connection info (ip:port,ip:port, ...). E.g., "
              "\"192.168.0.1:12000,192.168.0.2:12001\"");
DEFINE_int32(port, 12000, "The server listen port (TCP).");

DEFINE_uint32(num_io_workers, kDefaultNumIoWorkers,
              "The number of I/O workers (number of CPUs dedicated to I/O).");

DEFINE_string(comm, "rdma",
              "The communication method to use (current support: [RDMA, TCP])");

std::vector<std::string> GetHostList(const std::string& str) {
  std::vector<std::string> result;
  std::stringstream s_stream(str);
  while (s_stream.good()) {
    std::string substr;
    std::getline(s_stream, substr, ',');
    result.push_back(substr);
  }
  return result;
}

void ParseHostPort(const std::string& str, std::string& host, int& port) {
  std::stringstream s_stream(str);
  std::string port_str;
  // TODO: dirty hardcode here.
  std::getline(s_stream, host, ':');
  std::getline(s_stream, port_str);
  port = std::stoi(port_str.c_str());
}

}  // namespace dialga
