#ifndef DIALGA_SOCKET_BUFFER_H_
#define DIALGA_SOCKET_BUFFER_H_
#include <glog/logging.h>

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <malloc.h>
#include <cstdlib>

/// for small buffers, typically smaller than megabytes, malloc and free is as
/// fast as several nanoseconds, so to avoid the mutex and thread
/// synchronization overhead, we do not write a buffer pool from scratch currently

namespace dialga {
namespace socket {

/**
 * \brief Buffer for socket sending and receiving
 *
 * It handles the sending of the data on this buffer by moving the pointer. It
 * may not own the memory.
 */
class Buffer {
 public:
  // default constructor
  Buffer()
      : ptr_{nullptr},
        cap_{0},
        msg_length_{0},
        bytes_handled_{0},
        is_owner_{false} {}

  explicit Buffer(size_t cap)
      : cap_{cap}, msg_length_{0}, bytes_handled_{0}, is_owner_{true} {
    // ptr_ = static_cast<char*>(malloc(size_));
    int rc = posix_memalign(reinterpret_cast<void**>(&ptr_), 4096, cap);
    CHECK_EQ(rc, 0) << "posix_memalign failed";
  }

  explicit Buffer(const void* ptr, size_t cap, uint32_t msg_length)
      : Buffer(const_cast<void*>(ptr), cap, msg_length) {}

  explicit Buffer(void* ptr, size_t cap, uint32_t msg_length)
      : ptr_{static_cast<char*>(ptr)},
        cap_{cap},
        msg_length_{msg_length},
        bytes_handled_{0},
        is_owner_{false} {}

  explicit Buffer(Buffer* buffer)
      : Buffer(buffer->ptr_, buffer->cap_, buffer->msg_length_) {}

  ~Buffer() {
    if (is_owner_ && ptr_) {
      free(ptr_);
      ptr_ = nullptr;
    }
  }

  inline char* Chunk() { return ptr_ + bytes_handled_; }

  inline uint32_t Remaining() { return msg_length_ - bytes_handled_; }

  inline void Advance(size_t nbytes) { bytes_handled_ += nbytes; }

  inline bool HasRemaining() { return msg_length_ == bytes_handled_; }

  inline void set_msg_length(uint32_t msg_length) { msg_length_ = msg_length; }

  inline uint32_t msg_length() const { return msg_length_; }

  inline const char* ptr() const { return ptr_; }

  inline char* ptr() { return ptr_; }

  inline size_t capacity() { return cap_; }

  void CopyFrom(Buffer* other) {
    CHECK_GE(cap_, other->msg_length_);
    memcpy(ptr_, other->ptr_, other->msg_length_);
    msg_length_ = other->msg_length_;
  }

 private:
  char* ptr_;
  // buffer capacity
  size_t cap_;
  // valid data length
  uint32_t msg_length_;
  // bytes already handled
  uint32_t bytes_handled_;
  // whether this memory is owned
  bool is_owner_;
};

}  // namespace socket
}  // namespace dialga

#endif  // DIALGA_SOCKET_BUFFER_H_
