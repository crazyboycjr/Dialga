#ifndef PRISM_THREAD_PROTO_H_
#define PRISM_THREAD_PROTO_H_
#include <errno.h>
#include <fcntl.h>

#include <atomic>
#include <memory>
#include <thread>

class ThreadProto {
 public:
  virtual ~ThreadProto() noexcept {}

  virtual void Start() {
    this_thread_ = std::make_unique<std::thread>(&ThreadProto::Run, this);
  }

  virtual void Join() {  // NOLINT(*)
    if (this_thread_) this_thread_->join();
  }

 protected:
  virtual void Run() = 0;

  std::unique_ptr<std::thread> this_thread_;
};

class TerminableThread : public ThreadProto {
 public:
  TerminableThread() : terminated_{false} {}

  virtual void Terminate() {
    terminated_.store(true);
  }

  virtual void Join() override {
    ThreadProto::Join();
  }

 protected:
  std::atomic<bool> terminated_;
};

#endif  // PRISM_THREAD_PROTO_H_
