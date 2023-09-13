// Copyright [2023] Alibaba Cloud All rights reserved
#ifndef PAGE_ENGINE_DUMMY_ENGINE_H_
#define PAGE_ENGINE_DUMMY_ENGINE_H_
#include<string.h>
#include "page_engine.h"
#include "zstd.h"

#include <mutex>         //unique_lock
#include <shared_mutex>  //shared_mutex shared_lock
#include <thread>
/*
 * Dummy sample of page engine
 */

class DummyEngine : public PageEngine  {
 private:
  //这些资源是单独的不是共享的。宕机会摧毁，也需要持久化1
  short size_map[655362];//我的理解要用两个map来存。一个是交互4096，一个是真实的大小，但是zstd居然处理好了的。用交互4096来解压也没报错
  short real_size_map[655362];
  std::mutex real_size_lock;
  std::mutex size_lock;

  int fd;
  const size_t page_size{16384};
 public:
  static RetCode Open(const std::string& path, PageEngine** eptr);

  explicit DummyEngine(const std::string& path);

  ~DummyEngine() override;

  RetCode pageWrite(uint32_t page_no, const void *buf) override;

  RetCode pageRead(uint32_t page_no, void *buf) override;
};

#endif  // PAGE_ENGINE_DUMMY_ENGINE_H_
