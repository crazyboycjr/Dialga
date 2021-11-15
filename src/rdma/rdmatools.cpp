#include "./rdmatools.hpp"

#include <arpa/inet.h>
#include <gflags/gflags.h>
#include <infiniband/verbs.h>
#include <malloc.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include "dialga/config.hpp"

namespace dialga {

DEFINE_string(dev, "mlx5_0", "IB device name");
DEFINE_int32(gid, 3, "IB device gid index");
DEFINE_int32(mr_num, 1,
             "Initial number of memory regions");  // initial number of mr.
DEFINE_int32(
    buf_num, 65536,
    "Initial number of buffer per MR");  // initial number of buffer per mr.
DEFINE_uint32(buf_size, 8000, "Buffer size each memory chunk");
DEFINE_bool(share_cq, true, "All QPs share the same cq");
DEFINE_bool(event, false, "True if event-based, False for busy-polling");
DEFINE_int32(cq_depth, 65536, "The depth of Completion Queue");
DEFINE_int32(send_wq_depth, 1024, "The depth of send wq depth");
DEFINE_int32(recv_wq_depth, 1024, "The depth of recv wq depth");
DEFINE_int32(tcp_retry, 10, "TCP connection retry times");

DEFINE_int32(min_rnr_timer, 14, "Minimal Receive Not Ready error");
DEFINE_int32(hop_limit, 16, "Hop limit");
DEFINE_int32(tos, 0, "Type of Service value");
DEFINE_int32(qp_timeout, 0, "QP timeout value");
DEFINE_int32(retry_cnt, 7, "QP retry count");
DEFINE_int32(rnr_retry, 7, "Receive Not Ready retry count");
DEFINE_int32(max_qp_rd_atom, 16, "max_qp_rd_atom");
DEFINE_int32(mtu, IBV_MTU_1024,
             "IBV_MTU value:\n\
                    \t 1 indicates 256\n\
                    \t 2 indicates 512\n\
                    \t 3 indicates 1024(default)\n\
                    \t 4 indicates 2048\n\
                    \t 5 indicates 4096");

size_t RdmaMemory::ShapeSize(size_t size) {
  size_t ret;
  size = size + kCtrlMsgSize;
  if (size <= 4096)
    ret = 4096;
  else if (size <= 8192)
    ret = 8192;
  else if (size <= 65536)
    ret = 65536;
  else
    ret = 1048576;  // The largest value's size is limited as 1 MB.
  return ret;
}

int RdmaMemory::Malloc(int num) {
  int size = ShapeSize(size_);
  auto buf_size = size * num;
  int mrflags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
  char* buffer = nullptr;
  buffer = (char*)memalign(sysconf(_SC_PAGESIZE), buf_size);
  if (!buffer) {
    PLOG(ERROR) << "Memory Allocation failed";
    return -1;
  }
  mr_ = ibv_reg_mr(pd_, buffer, buf_size, mrflags);
  if (!mr_) {
    PLOG(ERROR) << "ibv_reg_mr() failed";
    return -1;
  }
  for (int i = 0; i < num; i++) {
    RdmaBuffer* rbuf = new RdmaBuffer((uint64_t)(buffer + size * i), size,
                                      mr_->lkey, mr_->rkey);
    buffers_.push(rbuf);
  }
  LOG(INFO) << "Memory registration success: size " << buf_size << " , mr addr "
            << mr_;
  return 0;
}

RdmaBuffer* RdmaMemory::GetBuffer(size_t size) {
  if (size > size_) return nullptr;
  if (buffers_.empty()) return nullptr;
  RdmaBuffer* buf = buffers_.front();
  buffers_.pop();
  return buf;
}

bool RdmaMemory::MatchBuffer(RdmaBuffer* buf) {
  if (buf->lkey_ != mr_->lkey || buf->rkey_ != mr_->rkey) {
    LOG(ERROR) << "MatchBuffer failed";
  }
  return (buf->lkey_ == mr_->lkey && buf->rkey_ == mr_->rkey);
}

void RdmaMemory::ReturnBuffer(RdmaBuffer* buf) { buffers_.push(buf); }

int RdmaManager::InitDevice() {
  struct ibv_device* dev = nullptr;
  struct ibv_device** device_list = nullptr;
  int n;
  device_list = ibv_get_device_list(&n);
  if (!device_list) {
    PLOG(ERROR) << "ibv_get_device_list() failed";
    return -1;
  }
  for (int i = 0; i < n; i++) {
    dev = device_list[i];
    if (!strncmp(ibv_get_device_name(dev), devname_.c_str(),
                 strlen(devname_.c_str())))
      break;
    else
      dev = nullptr;
  }
  if (dev) {
    ctx_ = ibv_open_device(dev);
    if (!ctx_) PLOG(ERROR) << "ibv_open_device() failed";
  } else {
    LOG(ERROR) << "We didn't find device " << devname_ << " and exit..";
  }
  ibv_free_device_list(device_list);
  if (!ctx_) return -1;
  // After open device. We query the gid first.
  if (ibv_query_gid(ctx_, 1, FLAGS_gid, &gid_) < 0) {
    PLOG(ERROR) << "ibv_query_gid() failed";
    return -1;
  }
  // All objects in our application shouldn't be malicious. Share pd for all of
  // them.
  pd_ = ibv_alloc_pd(ctx_);
  if (!pd_) {
    PLOG(ERROR) << "ibv_alloc_pd() failed";
    return -1;
  }
  if (FLAGS_event) {
    global_channel_ = ibv_create_comp_channel(ctx_);
    if (!global_channel_) {
      PLOG(ERROR) << "ibv_create_comp_channel() failed";
      return -1;
    }
  }
  if (FLAGS_share_cq) {
    global_cq_ =
        ibv_create_cq(ctx_, FLAGS_cq_depth, nullptr, global_channel_, 0);
    if (ibv_req_notify_cq(global_cq_, 0)) {
      PLOG(ERROR) << "ibv_req_notify_cq() failed";
      return -1;
    }
    if (!global_cq_) {
      PLOG(ERROR) << "ibv_create_cq() failed";
      return -1;
    }
  }
  return 0;
}

int RdmaManager::InitMemory() {
  for (int i = 0; i < FLAGS_mr_num; i++) {
    RdmaMemory* rdma_memory = nullptr;
    rdma_memory = new RdmaMemory(pd_, FLAGS_buf_size);
    int ret = rdma_memory->Malloc(FLAGS_buf_num);
    memory_pools_.push_back(rdma_memory);
    if (ret < 0) return -1;
  }
  return 0;
}

RdmaBuffer* RdmaManager::AllocateBuffer(size_t size) {
  RdmaBuffer* buf = nullptr;
  memory_lock_.lock();
  for (auto buffers : memory_pools_) {
    buf = buffers->GetBuffer(size);
    if (buf) break;
    // allocate success
  }
  memory_lock_.unlock();
  // We should promise that buffer is enough
  return buf;
}

void RdmaManager::FreeBuffer(RdmaBuffer* rdma_buffer) {
  memory_lock_.lock();
  for (auto buffers : memory_pools_) {
    if (buffers->MatchBuffer(rdma_buffer)) {
      buffers->ReturnBuffer(rdma_buffer);
      break;
    }
  }
  memory_lock_.unlock();
}

struct ibv_mr* RdmaManager::RegisterMemory(char* buf, size_t size) {
  int mrflags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
  auto mr = ibv_reg_mr(pd_, buf, size, mrflags);
  if (!mr) {
    PLOG(ERROR) << "ibv_reg_mr() failed";
    return nullptr;
  }
  return mr;
}

int RdmaManager::WaitForEvent() {
  struct ibv_cq* ev_cq;
  void* ev_ctx;
  int ret = ibv_get_cq_event(global_channel_, &ev_cq, &ev_ctx);
  if (ret < 0) {
    PLOG(ERROR) << "ibv_get_cq_event() failed";
    return -1;
  }
  ibv_ack_cq_events(ev_cq, 1);
  ret = ibv_req_notify_cq(ev_cq, 0);
  if (ret) {
    PLOG(ERROR) << "ibv_req_notify_cq() failed";
    return -1;
  }
  return 0;
}

int RdmaManager::TcpConnect(std::string host, int port) {
  struct addrinfo *res, *t;
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  char* service;
  int n;
  int sockfd = -1;
  if (asprintf(&service, "%d", port) < 0) return -1;
  n = getaddrinfo(host.c_str(), service, &hints, &res);
  if (n < 0) {
    LOG(ERROR) << gai_strerror(n) << " for " << host << ":" << port;
    free(service);
    return -1;
  }
  for (t = res; t; t = t->ai_next) {
    sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
    if (sockfd >= 0) {
      if (!connect(sockfd, t->ai_addr, t->ai_addrlen)) break;
      close(sockfd);
      sockfd = -1;
    }
  }
  freeaddrinfo(res);
  free(service);
  if (sockfd < 0) {
    LOG(ERROR) << "Couldn't connect to " << host << ":" << port;
    return -1;
  }
  return sockfd;
}

RdmaConnection* RdmaManager::Connect(std::string host, int port) {
  RdmaConnection* conn = nullptr;
  // 1. Designate CQ.
  struct ibv_cq* cq;
  if (!global_cq_) {
    // no share_cq
    cq = ibv_create_cq(ctx_, FLAGS_cq_depth, nullptr, global_channel_, 0);
    if (!cq) {
      PLOG(ERROR) << "ibv_create_cq() failed";
      return nullptr;
    }
  } else
    cq = global_cq_;
  // 2. Create the qp
  auto qp_init_attr = MakeQpInitAttr(cq, cq);
  int remote_qpn, attr_mask;
  struct ibv_qp_attr attr;
  auto qp = ibv_create_qp(pd_, &qp_init_attr);
  if (!qp) {
    PLOG(ERROR) << "ibv_create_qp() failed";
    return nullptr;
  }
  // 3. Connect to the remote host, exchange qp_num and gid.
  int fd = -1, n = -1;
  for (int i = 0; i < FLAGS_tcp_retry; i++) {
    fd = TcpConnect(host, port);
    if (fd > 0) break;
    LOG(INFO) << "Try connect to " << host << ":" << port << " failed for "
              << i + 1 << " times.....";
    sleep(1);
  }
  if (fd < 0) return nullptr;
  ConnectionMeta metadata_;
  metadata_.qp_num = htonl(qp->qp_num);
  memcpy(&metadata_.gid, &gid_, sizeof(union ibv_gid));
  if (write(fd, &metadata_, sizeof(metadata_)) != sizeof(metadata_)) {
    LOG(ERROR) << "Couldn't send local connection info";
    goto out;
  }
  n = read(fd, &metadata_, sizeof(metadata_));
  if (n != sizeof(metadata_)) {
    PLOG(ERROR) << "Client read";
    LOG(ERROR) << "Read only " << n << "/" << sizeof(metadata_) << " bytes";
    goto out;
  }
  // 4. metadata_ contains remote qpn and gid now. Let's modify QP to RTS
  remote_qpn = ntohl(metadata_.qp_num);
  attr = MakeQpAttr(IBV_QPS_INIT, IBV_QPT_RC, 0, metadata_.gid, &attr_mask);
  if (ibv_modify_qp(qp, &attr, attr_mask)) {
    PLOG(ERROR) << "Failed to modify QP to INIT";
    goto out;
  }
  attr = MakeQpAttr(IBV_QPS_RTR, IBV_QPT_RC, remote_qpn, metadata_.gid,
                    &attr_mask);
  if (ibv_modify_qp(qp, &attr, attr_mask)) {
    PLOG(ERROR) << "Failed to modify QP to RTR";
    goto out;
  }
  attr = MakeQpAttr(IBV_QPS_RTS, IBV_QPT_RC, remote_qpn, metadata_.gid,
                    &attr_mask);
  if (ibv_modify_qp(qp, &attr, attr_mask)) {
    PLOG(ERROR) << "Failed to modify QP to RTS";
    goto out;
  }
  // 5. QP now is ready.
  LOG(INFO) << "The CQ is " << cq;
  LOG(INFO) << "qp->send_cq is " << qp->send_cq;
  LOG(INFO) << "qp->recv_cq is " << qp->recv_cq;
  conn = new RdmaConnection(qp);
out:
  close(fd);
  return conn;
}

RdmaConnection* RdmaManager::TcpServe(int fd) {
  RdmaConnection* conn = nullptr;
  // 1. Designate CQ.
  struct ibv_cq* cq;
  if (!global_cq_) {
    // no share_cq
    cq = ibv_create_cq(ctx_, FLAGS_cq_depth, nullptr, global_channel_, 0);
    if (!cq) {
      PLOG(ERROR) << "ibv_create_cq() failed";
      return nullptr;
    }
  } else
    cq = global_cq_;
  // 2. Create the qp
  auto qp_init_attr = MakeQpInitAttr(cq, cq);
  int remote_qpn, attr_mask;
  struct ibv_qp_attr attr;
  auto qp = ibv_create_qp(pd_, &qp_init_attr);
  if (!qp) {
    PLOG(ERROR) << "ibv_create_qp() failed";
    return nullptr;
  }
  LOG(INFO) << "The CQ is " << cq;
  LOG(INFO) << "qp->send_cq is " << qp->send_cq;
  LOG(INFO) << "qp->recv_cq is " << qp->recv_cq;
  // 3. Read from the fd and set up connection.
  ConnectionMeta metadata_;
  int n;
  n = read(fd, &metadata_, sizeof(metadata_));
  if (n != sizeof(metadata_)) {
    PLOG(ERROR) << "Server read";
    LOG(ERROR) << "Read only " << n << "/" << sizeof(metadata_) << " bytes";
    goto out;
  }
  remote_qpn = ntohl(metadata_.qp_num);
  attr = MakeQpAttr(IBV_QPS_INIT, IBV_QPT_RC, 0, metadata_.gid, &attr_mask);
  if (ibv_modify_qp(qp, &attr, attr_mask)) {
    PLOG(ERROR) << "Failed to modify QP to INIT";
    goto out;
  }
  attr = MakeQpAttr(IBV_QPS_RTR, IBV_QPT_RC, remote_qpn, metadata_.gid,
                    &attr_mask);
  if (ibv_modify_qp(qp, &attr, attr_mask)) {
    PLOG(ERROR) << "Failed to modify QP to RTR";
    goto out;
  }
  attr = MakeQpAttr(IBV_QPS_RTS, IBV_QPT_RC, remote_qpn, metadata_.gid,
                    &attr_mask);
  if (ibv_modify_qp(qp, &attr, attr_mask)) {
    PLOG(ERROR) << "Failed to modify QP to RTS";
    goto out;
  }
  conn = new RdmaConnection(qp);
  return conn;
out:
  return nullptr;
}

void RdmaManager::TcpAck(int fd, struct ibv_qp* qp) {
  ConnectionMeta metadata_;
  metadata_.qp_num = htonl(qp->qp_num);
  memcpy(&metadata_.gid, &gid_, sizeof(union ibv_gid));
  if (write(fd, &metadata_, sizeof(metadata_)) != sizeof(metadata_)) {
    LOG(ERROR) << "Couldn't send local connection info";
  }
  close(fd);
  return;
}

struct ibv_qp_init_attr MakeQpInitAttr(struct ibv_cq* send_cq,
                                       struct ibv_cq* recv_cq) {
  struct ibv_qp_init_attr qp_init_attr;
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.sq_sig_all = 0;
  qp_init_attr.send_cq = send_cq;
  qp_init_attr.recv_cq = recv_cq;
  qp_init_attr.cap.max_send_wr = FLAGS_send_wq_depth;
  qp_init_attr.cap.max_recv_wr = FLAGS_recv_wq_depth;
  qp_init_attr.cap.max_send_sge = kMaxSge;
  qp_init_attr.cap.max_recv_sge = kMaxSge;
  qp_init_attr.cap.max_inline_data = kMaxInline;
  return qp_init_attr;
}

struct ibv_qp_attr MakeQpAttr(enum ibv_qp_state state, enum ibv_qp_type qp_type,
                              int remote_qpn, const union ibv_gid& remote_gid,
                              int* attr_mask) {
  struct ibv_qp_attr attr;
  memset(&attr, 0, sizeof(attr));
  *attr_mask = 0;
  switch (state) {
    case IBV_QPS_INIT:
      attr.port_num = 1;
      attr.qp_state = IBV_QPS_INIT;
      switch (qp_type) {
        case IBV_QPT_UD:
          attr.qkey = 0;
          *attr_mask |= IBV_QP_QKEY;
          break;
        case IBV_QPT_UC:
        case IBV_QPT_RC:
          attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE |
                                 IBV_ACCESS_REMOTE_READ |
                                 IBV_ACCESS_REMOTE_ATOMIC;
          *attr_mask |= IBV_QP_ACCESS_FLAGS;
          break;
        default:
          LOG(ERROR) << "Unsupported QP type: " << qp_type;
          break;
      }
      *attr_mask |= IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT;
      break;
    case IBV_QPS_RTR:
      attr.qp_state = IBV_QPS_RTR;
      *attr_mask |= IBV_QP_STATE;
      switch (qp_type) {
        case IBV_QPT_RC:
          attr.max_dest_rd_atomic = FLAGS_max_qp_rd_atom;
          attr.min_rnr_timer = FLAGS_min_rnr_timer;
          *attr_mask |= IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
        case IBV_QPT_UC:
          attr.path_mtu = (enum ibv_mtu)FLAGS_mtu;
          attr.dest_qp_num = remote_qpn;
          attr.rq_psn = 0;
          attr.ah_attr.is_global = 1;
          attr.ah_attr.grh.flow_label = 0;
          attr.ah_attr.grh.sgid_index = FLAGS_gid;
          attr.ah_attr.grh.hop_limit = FLAGS_hop_limit;
          attr.ah_attr.grh.traffic_class = FLAGS_tos;
          memcpy(&attr.ah_attr.grh.dgid, &remote_gid, 16);
          attr.ah_attr.dlid = 0;
          attr.ah_attr.sl = 0;
          attr.ah_attr.src_path_bits = 0;
          attr.ah_attr.port_num = 1;
          *attr_mask |=
              IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;
          break;
        case IBV_QPT_UD:
          break;
      }
      break;
    case IBV_QPS_RTS:
      attr.qp_state = IBV_QPS_RTS;
      attr.sq_psn = 0;
      *attr_mask |= IBV_QP_STATE | IBV_QP_SQ_PSN;
      switch (qp_type) {
        case IBV_QPT_RC:
          attr.timeout = FLAGS_qp_timeout;
          attr.retry_cnt = FLAGS_retry_cnt;
          attr.rnr_retry = FLAGS_rnr_retry;  // This is the retry counter, 7
                                             // means that try infinitely.
          attr.max_rd_atomic = FLAGS_max_qp_rd_atom;
          *attr_mask |= IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                        IBV_QP_MAX_QP_RD_ATOMIC;
        case IBV_QPT_UC:
        case IBV_QPT_UD:
          break;
      }
      break;
    default:
      break;
  }
  return attr;
}

bool RdmaConnection::AcquireSendCredits(int num) {
  auto send_credits = send_credits_.load();
  if (send_credits < num) return false;
  auto updated_value = send_credits - num;
  return send_credits_.compare_exchange_strong(send_credits, updated_value);
}

bool RdmaConnection::AcquireRecvCredits(int num) {
  auto recv_credits = recv_credits_.load();
  if (recv_credits < num) return false;
  auto updated_value = recv_credits - num;
  return recv_credits_.compare_exchange_strong(recv_credits, updated_value);
}

void RdmaConnection::UpdateSendCredits(int credits) {
  send_credits_.fetch_add(credits);
}

void RdmaConnection::UpdateRecvCredits(int credits) {
  recv_credits_.fetch_add(credits);
}

}  // namespace dialga
