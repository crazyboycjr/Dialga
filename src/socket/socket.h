#ifndef DIALGA_SOCKET_SOCKET_H_
#define DIALGA_SOCKET_SOCKET_H_

#include <glog/logging.h>
#include "prism/utils.h"
#include <netdb.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

namespace dialga {
namespace socket {

using SOCKET = int;
const SOCKET INVALID_SOCKET = -1;

struct AddrInfo {
  struct addrinfo* ai;

  AddrInfo(uint16_t port, int protocol) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = protocol;
    hints.ai_flags = AI_PASSIVE | AI_V4MAPPED | AI_ADDRCONFIG;
    {
      int err = getaddrinfo(nullptr, std::to_string(port).c_str(), &hints, &ai);
      CHECK(!err) << "getaddrinfo: " << gai_strerror(err);
    }
  }
  AddrInfo(const char* host, uint16_t port, int protocol, bool passive) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = protocol;
    hints.ai_flags = (passive ? AI_PASSIVE : 0) | AI_V4MAPPED | AI_ADDRCONFIG;
    {
      int err = getaddrinfo(host, std::to_string(port).c_str(), &hints, &ai);
      CHECK(!err) << "getaddrinfo: " << gai_strerror(err);
    }
  }
  AddrInfo(const char* host, uint16_t port, int protocol)
      : AddrInfo(host, port, protocol, false) {}
  ~AddrInfo() {
    if (ai) freeaddrinfo(ai);
  }
  static std::string AddrStr(struct sockaddr* addr, socklen_t len, int protocol = SOCK_STREAM) {
    char addr_buf[NI_MAXHOST];
    char serv_buf[NI_MAXSERV];
    int flags = NI_NUMERICHOST | NI_NUMERICSERV;
    {
      int err = getnameinfo(addr, len, addr_buf, sizeof(addr_buf), serv_buf,
                            sizeof(serv_buf), flags);
      CHECK(!err) << "getnameinfo: " << gai_strerror(err);
    }
    auto proto_str = protocol == SOCK_STREAM ? "tcp" : "udp";
    return prism::FormatString("%s://%s:%s", proto_str, addr_buf, serv_buf);
  }
  std::string AddrStr() {
    CHECK(ai != nullptr);
    struct sockaddr* addr = ai->ai_addr;
    socklen_t len = ai->ai_addrlen;
    return AddrStr(addr, len, ai->ai_socktype);
  }
};

struct SockAddr {
  union {
    struct sockaddr addr;
    struct sockaddr_in addr_in;
    struct sockaddr_in6 addr_in6;
    struct sockaddr_storage addr_storage;
  };
  socklen_t addrlen;

  SockAddr() : addrlen(sizeof(struct sockaddr_storage)) {
  }

  SockAddr(const struct sockaddr* saddr, socklen_t len) {
    CHECK(saddr && len > 0) << "SockAddr initialized from the empty";
    memcpy(&addr, saddr, len);
    addrlen = len;
  }

  SockAddr(const struct AddrInfo& ai) : SockAddr(ai.ai->ai_addr, ai.ai->ai_addrlen) {
  }

  std::string AddrStr(int protocol = SOCK_STREAM) {
    return AddrInfo::AddrStr(&addr, addrlen, protocol);
  }
};

class Socket {
 public:
  SOCKET sockfd;

  explicit Socket(SOCKET sockfd) : sockfd(sockfd) {}

  inline operator SOCKET() const {
    return sockfd;
  }

  inline static int GetLastError() {
    return errno;
  }

  inline static bool LastErrorWouldBlock() {
    int errsv = GetLastError();
    return errsv == EAGAIN || errsv == EWOULDBLOCK;
  }

  inline int GetSockError() const {
    int error = 0;
    socklen_t len = sizeof(error);
    PCHECK(!getsockopt(sockfd,  SOL_SOCKET, SO_ERROR, &error, &len));
    return error;
  }

  inline void SetNonBlock(bool non_block) {
    int flags;
    flags = fcntl(sockfd, F_GETFL);
    if (non_block) {
      flags |= O_NONBLOCK;
    } else {
      flags &= ~O_NONBLOCK;
    }
    PCHECK(fcntl(sockfd, F_SETFL, flags) != -1);
  }

  inline void SetReuseAddr(bool reuse) {
    int val = reuse ? 1 : 0;
    PCHECK(!setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)));
  }

  inline void SetRecvBuffer(int bufsize) {
    int val = bufsize;
    PCHECK(!setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &val, sizeof(val)));
  }

  inline void SetSendBuffer(int bufsize) {
    int val = bufsize;
    PCHECK(!setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &val, sizeof(val)));
  }

  inline void GetRecvBuffer(int* bufsize) {
    socklen_t optlen;
    PCHECK(!getsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, bufsize, &optlen));
  }

  inline void GetSendBuffer(int* bufsize) {
    socklen_t optlen;
    PCHECK(!getsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, bufsize, &optlen));
  }

  inline void SetTos(uint8_t tos) {
    uint8_t val = tos;
    PCHECK(!setsockopt(sockfd, IPPROTO_IP, IP_TOS, &val, sizeof(val)));
  }

  inline bool IsClosed() const {
    return sockfd == INVALID_SOCKET;
  }

  inline void Close() {
    CHECK_NE(sockfd, INVALID_SOCKET) << "double close the socket or close without create";
    close(sockfd);
    sockfd = INVALID_SOCKET;
  }

  inline void Shutdown(int how) {
    /// how in {SHUT_RD, SHUT_WR, SHUT_RDWR}
    PCHECK(!shutdown(sockfd, how));
  }

  inline void Bind(const AddrInfo& ai) {
    Bind(ai.ai->ai_addr, ai.ai->ai_addrlen);
  }

  inline void Bind(const sockaddr* addr, socklen_t len) {
    PCHECK(!bind(sockfd, addr, len));
  }

  inline bool TryBind(const AddrInfo& ai) {
    return TryBind(ai.ai->ai_addr, ai.ai->ai_addrlen);
  }

  inline bool TryBind(const sockaddr* addr, socklen_t len) {
    return bind(sockfd, addr, len) == 0;
  }
};

class TcpSocket : public Socket {
 public:
  TcpSocket() : Socket(INVALID_SOCKET) {
  }

  explicit TcpSocket(SOCKET sockfd) : Socket(sockfd) {
  }

  inline void Create(const AddrInfo& ai) {
    sockfd = ::socket(ai.ai->ai_family, ai.ai->ai_socktype, ai.ai->ai_protocol);
    PCHECK(sockfd != INVALID_SOCKET) << "create socket failed";
  }

  inline void Create(int domain = AF_INET) {
    sockfd = ::socket(domain, SOCK_STREAM, IPPROTO_TCP);
    PCHECK(sockfd != INVALID_SOCKET) << "create socket failed";
  }

  inline void Listen(int backlog = 128) {
    PCHECK(!listen(sockfd, backlog));
  }

  TcpSocket Accept() {
    SOCKET new_sock = accept(sockfd, nullptr, nullptr);
    PCHECK(new_sock != INVALID_SOCKET) << "accept";
    return TcpSocket(new_sock);
  }

  inline bool Connect(const AddrInfo& ai) {
    SockAddr sock_addr(ai);
    return Connect(sock_addr);
  }

  inline bool Connect(const SockAddr& addr) {
    return connect(sockfd, &addr.addr, sizeof(addr.addr)) == 0;
  }

  inline ssize_t Send(const void* buf, size_t len, int flags = 0) {
    return send(sockfd, buf, len, flags);
  }

  inline ssize_t Recv(void* buf, size_t len, int flags = 0) {
    return recv(sockfd, buf, len, flags);
  }

  inline size_t SendAll(const void* buf, size_t len, int flags = 0) {
    const char* cbuf = static_cast<const char*>(buf);
    size_t n = 0;
    while (n < len) {
      ssize_t r = Send(cbuf + n, len - n, flags);
      if (r == -1) {
        if (LastErrorWouldBlock()) continue;
        LOG(ERROR) << "socket error: " << GetSockError();
        break;
      } else {
        n += r;
      }
    }
    return n;
  }

  inline size_t RecvAll(void* buf, size_t len, int flags = 0) {
    char* cbuf = static_cast<char*>(buf);
    size_t n = 0;
    while (n < len) {
      ssize_t r = Recv(cbuf + n, len - n, flags);
      if (r == -1) {
        if (LastErrorWouldBlock()) continue;
        LOG(ERROR) << "socket error: " << GetSockError();
        break;
      } else if (r == 0) {
        break;
      } else {
        n += r;
      }
    }
    return n;
  }
};

class UdpSocket : public Socket {
 public:
  UdpSocket() : Socket(INVALID_SOCKET) {
  }

  explicit UdpSocket(SOCKET sockfd) : Socket(sockfd) {
  }

  inline void Create(const AddrInfo& ai) {
    sockfd = ::socket(ai.ai->ai_family, ai.ai->ai_socktype, ai.ai->ai_protocol);
    PCHECK(sockfd != INVALID_SOCKET) << "create socket failed";
  }

  inline void Create(int domain = AF_INET) {
    sockfd = ::socket(domain, SOCK_DGRAM, IPPROTO_UDP);
    PCHECK(sockfd != INVALID_SOCKET) << "create socket failed";
  }

  inline ssize_t SendTo(const void* buf, size_t len, int flags, const SockAddr& addr) {
    return sendto(sockfd, buf, len, flags, &addr.addr, addr.addrlen);
  }

  inline ssize_t RecvFrom(void* buf, size_t len, int flags) {
    return recvfrom(sockfd, buf, len, flags, nullptr, nullptr);
  }

  inline ssize_t RecvFrom(void* buf, size_t len, int flags, SockAddr* addr) {
    return recvfrom(sockfd, buf, len, flags, &addr->addr, &addr->addrlen);
  }

  inline ssize_t SendMsg(const struct msghdr* msg, int flags) {
    return sendmsg(sockfd, msg, flags);
  }

  inline ssize_t RecvMsg(struct msghdr* msg, int flags) {
    return recvmsg(sockfd, msg, flags);
  }
};

}  // namespace socket
}  // namespace dialga

#endif  // DIALGA_SOCKET_SOCKET_H_
