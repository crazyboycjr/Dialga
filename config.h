#ifndef KVSTORE_CONFIG_H_
#define KVSTORE_CONFIG_H_
#include <cstdint>
#include <optional>
#include <string>
#include <sstream>

namespace kvstore {

/// Client side configuration.
const uint16_t kDefaultPort = 6000;

struct Configuration {
  uint16_t port;
  std::optional<std::string> host;
};

/// Server side configuration.
const uint16_t kDefaultNumCPUs = 1;

struct ServerConfig {
  uint16_t port;
  size_t num_cpus;
};

}  // namespace kvstore

#endif  // KVSTORE_CONFIG_H_
