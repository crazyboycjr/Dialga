#ifndef DIALGA_SOCKET_POLL_H_
#define DIALGA_SOCKET_POLL_H_
#include <sys/epoll.h>
#include <optional>

#include "socket/socket.h"
#include <glog/logging.h>

namespace dialag {
namespace socket {

class Interest {
 public:
  enum : uint8_t {
    READABLE = 0b01,
    WRITABLE = 0b10,
  };
  /*!
   * \brief Interest can be constructed through Interest::READABLE and
   * Interest::WRITABLE with implicit type conversion.
   */
  Interest(uint8_t interest) {
    CHECK(interest <= (READABLE | WRITABLE));
    inner_ = interest;
  }
  inline operator uint8_t() const {
    return inner_;
  }
  inline bool IsReadable() const {
    return inner_ & READABLE;
  }
  inline bool IsWritable() const {
    return inner_ & WRITABLE;
  }
  inline Interest Add(const Interest other) const {
    return Interest(inner_ | other.inner_);
  }
  inline Interest operator|(const Interest other) const {
    return this->Add(other);
  }
  inline Interest& operator|=(const Interest other) {
    inner_ |= other.inner_;
    return *this;
  }
  std::string DebugString() const {
    std::stringstream ss;
    int count = 0;
    if (IsReadable()) {
      ss << "READABLE";
      count++;
    }
    if (IsWritable()) {
      ss << ((count >= 1) ? " | " : "") << "WRITABLE";
      count++;
    }
    return ss.str();
  }
 private:
  uint8_t inner_;
};

inline std::ostream& operator<<(std::ostream& os, const Interest interest) {
  os << interest.DebugString();
  return os;
}

struct Token {
  size_t token;
  Token(size_t value) : token{value} {}
  inline operator size_t() const {
    return token;
  }
};

class Event;

namespace sys {

namespace linux {
inline uint32_t InterestToEpoll(Interest interest) {
  /// EPOLLERR  Error condition happened on the associated file descriptor.  This event is also
  /// reported for the write end of a pipe when the read end has been closed. epoll_wait(2) will
  /// always report for this event; it is not necessary to set it in events when calling
  /// epoll_ctl().
  // let's just use level-trigger because it's less error prone and still efficient
  uint32_t kind = EPOLLET;
  // uint32_t kind = 0;
  if (interest.IsReadable()) {
    kind |= EPOLLIN | EPOLLRDHUP;
  }
  if (interest.IsWritable()) {
    kind |= EPOLLOUT;
  }
  return kind;
}

typedef int RawFd;
const RawFd INVALID_FD = -1;

class Selector {
 public:
  static Selector Create() {
    return Selector();
  }
  Selector() {
    epollfd_ = epoll_create1(EPOLL_CLOEXEC);
    PCHECK(epollfd_ >= 0) << "epoll_create1 error";
  }
  Selector(int flags) {
    epollfd_ = epoll_create1(flags);
    PCHECK(epollfd_ >= 0) << "epoll_create1 error, flags: " << flags;
  }
  Selector(const Selector& other) {
    epollfd_ = dup(other.epollfd_);
    PCHECK(epollfd_ >= 0) << "dup, other.epollfd_: " << other.epollfd_;
  }
  ~Selector() {
    if (epollfd_ != INVALID_FD) {
      close(epollfd_);
      epollfd_ = INVALID_FD;
    }
  }
  inline operator RawFd() const {
    return epollfd_;
  }
  inline void EpollCtl(int op, RawFd fd, struct epoll_event* epev) {
    PCHECK(epoll_ctl(epollfd_, op, fd, epev) == 0);
  }
  inline int Select(Event* events, int maxevents,
                    std::optional<std::chrono::milliseconds> timeout_ms) {
    int timeout = timeout_ms.has_value() ? timeout_ms.value().count() : -1;
    struct epoll_event* epev = reinterpret_cast<struct epoll_event*>(events);
    return epoll_wait(epollfd_, epev, maxevents, timeout);
  }
  inline void Register(RawFd fd, Token token, Interest interests) {
    struct epoll_event event = (struct epoll_event) {
      .events = InterestToEpoll(interests),
      .data {
        .u64 = token,
      },
    };
    EpollCtl(EPOLL_CTL_ADD, fd, &event);
  }
  inline void Reregister(RawFd fd, Token token, Interest interests) {
    struct epoll_event event = (struct epoll_event) {
      .events = InterestToEpoll(interests),
      .data {
        .u64 = token,
      },
    };
    EpollCtl(EPOLL_CTL_MOD, fd, &event);
  }
  inline void Deregister(RawFd fd) {
    /// In kernel versions before 2.6.9, the EPOLL_CTL_DEL operation required
    /// a non-null pointer in event, even though this argument is ignored.
    /// Since Linux 2.6.9, event can be specified as NULL  when  using
    /// EPOLL_CTL_DEL.  Applications that need to be portable to kernels
    /// before 2.6.9 should specify a non-null pointer in event.
    EpollCtl(EPOLL_CTL_DEL, fd, nullptr);
  }
 private:
  RawFd epollfd_{INVALID_FD};
};

// typedef union epoll_data
// {
//   void *ptr;
//   int fd;
//   uint32_t u32;
//   uint64_t u64;
// } epoll_data_t;

// struct epoll_event
// {
//   uint32_t events;	/* Epoll events */
//   epoll_data_t data;	/* User data variable */
// } __EPOLL_PACKED;

/*!
 * \brief a wrapper of struct epoll_event
 */
class Event {
 public:
  inline Token token() const {
    return Token(event_.data.u64);
  }
  inline bool IsReadable() const {
    return event_.events & EPOLLIN;
  }
  inline bool IsWritable() const {
    return event_.events & EPOLLOUT;
  }
  inline bool IsError() const {
    return event_.events & EPOLLERR;
  }
  inline bool IsReadClosed() const {
    return (event_.events & EPOLLHUP) ||
           ((event_.events & EPOLLRDHUP) && (event_.events & EPOLLIN));
  }
  inline bool IsWriteClosed() const {
    return (event_.events & EPOLLHUP) ||
           ((event_.events & EPOLLOUT) && (event_.events & EPOLLERR));
  }
  std::string DebugString() {
    std::stringstream ss;
    int count = 0;
    if (IsReadable()) {
      ss << "EPOLLIN";
      count++;
    }
    if (IsWritable()) {
      ss << ((count >= 1) ? " | " : "") << "EPOLLOUT";
      count++;
    }
    if (IsError()) {
      ss << ((count >= 1) ? " | " : "") << "EPOLLERR";
      count++;
    }
    ss << "{ events: " << event_.events
       << ", data: " << event_.data.u64 << " }";
    return ss.str();
  }
 private:
  struct epoll_event event_;
};

static_assert(sizeof(Event) == sizeof(struct epoll_event));

}  // namespace linux

#ifdef __linux__
using RawFd = linux::RawFd;
using Event = linux::Event;
using Selector = linux::Selector;
#elif __FreeBSD__
using RawFd = freebsd::RawFd;
using Event = freebsd::Event;
using Selector = freebsd::Selector;
#endif
}  // namespace sys

using sys::RawFd;

class Event {
 public:
  inline Token token() const {
    return inner_.token();
  }
  inline bool IsReadable() const {
    return inner_.IsReadable();
  }
  inline bool IsWritable() const {
    return inner_.IsWritable();
  }
  inline bool IsError() const {
    return inner_.IsError();
  }
  inline bool IsReadClosed() const {
    return inner_.IsReadClosed();
  }
  inline bool IsWriteClosed() const {
    return inner_.IsWriteClosed();
  }
  std::string DebugString() {
    return inner_.DebugString();
  }
 private:
  sys::Event inner_;
};

static_assert(sizeof(Event) == sizeof(sys::Event));

class Registry;

class EventSource {
 public:
  virtual void Register(const Registry& registry, Token token, Interest interests) = 0;
  virtual void Reregister(const Registry& registry, Token token, Interest interests) = 0;
  virtual void Deregister(const Registry& registry) = 0;
};

class Registry {
 public:
  friend class Poll;
  explicit Registry(sys::Selector selector) : selector_{selector} {}
  inline sys::Selector& selector() {
    return selector_;
  }
  template <typename S>
  inline void Register(S& source, Token token, Interest interests) {
    source.Register(*this, token, interests);
  }
  template <typename S>
  inline void Reregister(S& source, Token token, Interest interests) {
    source.Reregister(*this, token, interests);
  }
  template <typename S>
  inline void Deregister(S& source) {
    source.Deregister(*this);
  }
 private:
  sys::Selector selector_;
};

// template specification for class Socket
#define POLL_DEFINE_FUNC_REGISTER_SOCKET(type)                                             \
  template <>                                                                              \
  inline void Registry::Register<type>(type & socket, Token token, Interest interests) {   \
    selector_.Register(socket.sockfd, token, interests);                                   \
  }                                                                                        \
  template <>                                                                              \
  inline void Registry::Reregister<type>(type & socket, Token token, Interest interests) { \
    selector_.Reregister(socket.sockfd, token, interests);                                 \
  }                                                                                        \
  template <>                                                                              \
  inline void Registry::Deregister<type>(type & socket) {                                  \
    selector_.Deregister(socket.sockfd);                                                   \
  }

POLL_DEFINE_FUNC_REGISTER_SOCKET(Socket)
POLL_DEFINE_FUNC_REGISTER_SOCKET(TcpSocket)
POLL_DEFINE_FUNC_REGISTER_SOCKET(UdpSocket)

#undef POLL_DEFINE_FUNC_REGISTER_SOCKET

class Poll {
 public:
  static Poll Create() {
    return Poll { 
      Registry {
        sys::Selector::Create()
      }
    };
  }
  explicit Poll(Registry registry) : registry_{registry} {}
  inline operator RawFd() const {
    return registry_.selector_;
  }
  inline Registry& registry() {
    return registry_;
  }
  inline int PollUntil(Event* events, int maxevents,
                       std::optional<std::chrono::microseconds> timeout_ms) {
    return registry_.selector_.Select(events, maxevents, timeout_ms);
  }

 private:
  Registry registry_;
};

static_assert(sizeof(Poll) == sizeof(RawFd));

}  // namespace socket
}  // namespace dialga

#endif  // DIALGA_SOCKET_POLL_H_
