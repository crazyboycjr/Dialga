#ifndef KVSTORE_CONFIG_HPP_
#define KVSTORE_CONFIG_HPP_
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include <cstdint>
#include <optional>
#include <sstream>
#include <string>

namespace kvstore {

DECLARE_string(dev);
DECLARE_int32(gid);
DECLARE_int32(mr_num);    // initial number of mr. 
DECLARE_int32(buf_num);   // initial number of buffer per mr.
DECLARE_uint32(buf_size); // initial buffer size.
DECLARE_int32(port);

DECLARE_bool(share_cq);
DECLARE_bool(event); 
DECLARE_int32(cq_depth);
DECLARE_int32(send_wq_depth);
DECLARE_int32(recv_wq_depth);
DECLARE_int32(tcp_retry);

DECLARE_int32(min_rnr_timer);
DECLARE_int32(hop_limit);
DECLARE_int32(tos);
DECLARE_int32(qp_timeout);
DECLARE_int32(retry_cnt);
DECLARE_int32(rnr_retry);
DECLARE_int32(max_qp_rd_atom);
DECLARE_int32(mtu);

DECLARE_string(connect);

/// Client side configuration.
constexpr uint16_t kDefaultPort = 6000;
constexpr uint16_t kDefaultNumCPUs = 1;

constexpr int kMaxBatch = 128;
constexpr int kMaxSge = 16;
constexpr int kMaxInline = 512;
constexpr int kMaxCuncur = 48; // At most 48 QPs per server
constexpr int kCqPollDepth = 128;

}  // namespace kvstore

#endif  // KVSTORE_CONFIG_H_
