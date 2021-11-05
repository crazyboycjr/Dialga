#include "kvstore.hpp"
namespace kvstore {
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
}