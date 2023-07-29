// Copyright [2023] Alibaba Cloud All rights reserved
#ifndef PAGE_ENGINE_DUMMY_ENGINE_H_
#define PAGE_ENGINE_DUMMY_ENGINE_H_
#include "page_engine.h"
//#include <zlib.h>
#include<map>
#include<string.h>
/*
 * Dummy sample of page engine
 */

class DummyEngine : public PageEngine  {
 private:
  int fd;
  const size_t page_size{16384};
  std::map<uint32_t,unsigned short> size_map;//页号可以设置为unsigned short吗？感觉费劲，应该不行，他说的不超过655360，unsigned short最多才65535
  std::map<uint32_t,unsigned long long> offset_map;
  unsigned long long last_write=0;
  
  //unsigned long long sum=0;
 public:
  static RetCode Open(const std::string& path, PageEngine** eptr);

  explicit DummyEngine(const std::string& path);

  ~DummyEngine() override;
//这是原版的
  RetCode pageWrite(uint32_t page_no, const void *buf) override;
//  RetCode pageWrite(uint32_t page_no, void *buf) override;

  RetCode pageRead(uint32_t page_no, void *buf) override;
};

#endif  // PAGE_ENGINE_DUMMY_ENGINE_H_
