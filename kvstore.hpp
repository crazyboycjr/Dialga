#ifndef KVSTORE_KVSTORE_HPP_
#define KVSTORE_KVSTORE_HPP_
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "rdmatools.hpp"

namespace kvstore {

using Key = uint64_t;
using Value = std::string;

/* \brief: Callback to allow pipeline. */
using Callback = std::function<void()>;

class IndexEntry {
 public:
  IndexEntry(int qp_index, uint64_t addr, size_t size)
      : qp_index_(qp_index), addr_(addr), size_(size) {}
  // which host(qp) owns the content
  int qp_index_;
  // the virtual addr of the content
  uint64_t addr_;
  // the size of the content
  size_t size_;
};

class KVStore {
 public:
  virtual ~KVStore() {}

  static KVStore* Create(const char* type = "local");

  virtual int Init() = 0;

  virtual int Put(const std::vector<Key>& keys,
                   const std::vector<Value>& values,
                   const Callback& cb = nullptr) = 0;

  virtual int Get(const std::vector<Key>& keys,
                   const std::vector<Value*>& values,
                   const Callback& cb = nullptr) = 0;

  virtual int Delete(const std::vector<Key>& keys,
                      const Callback& cb = nullptr) = 0;

 protected:
  std::unordered_map<Key, IndexEntry> indexs_;
};

class RdmaKVStore : public KVStore {
 public:
  int Init();  // return 0 on success. If success, then can Put and Get.
  std::vector<std::string> GetHostList(const std::string&);
  void ParseHostPort(const std::string&, std::string&, int&);
  char* RdmaMalloc(size_t size);
  void RdmaFree(char *buf);
  int Put(const std::vector<Key>& keys, const std::vector<Value>& values,
           const Callback& cb = nullptr);
  int Get(const std::vector<Key>& keys, const std::vector<Value*>& values,
      const Callback& cb = nullptr);
  int FakePost(char *);
  int Delete(const std::vector<Key>& keys,
                      const Callback& cb = nullptr);

 private:
  RdmaManager* manager_ = nullptr;
  std::vector<RdmaConnection*> connections_;
  class TxMessage {
      public: 
        Key key;
        int opcode;     // 0 for NEW / 1 for PUT / 2 for GET / 3 for DEL
        uint64_t size;  // valid for NEW 
  };
};

class RdmaKVServer {
 public:
  int Init();
  int TcpListen();
  int AcceptHander(int fd, int idx);

 private:
  RdmaManager* manager_ = nullptr;
  std::vector<RdmaConnection*> connections_;
};

}  // namespace kvstore

#endif  // KVSTORE_KVSTORE_H_
