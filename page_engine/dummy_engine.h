// Copyright [2023] Alibaba Cloud All rights reserved
#ifndef PAGE_ENGINE_DUMMY_ENGINE_H_
#define PAGE_ENGINE_DUMMY_ENGINE_H_
#include "page_engine.h"
//#include <zlib.h>
#include<map>
#include<string.h>
#include "zlib.h"
/*
 * Dummy sample of page engine
 */
typedef struct FreeBlock
{
    long long offset;
    size_t size;
    struct FreeBlock *next;
}free_block;

class DummyEngine : public PageEngine  {
 private:
  int fd;
  const size_t page_size{16384};
  short size_map[655362];
  long long offset_map[655362];
  long long last_write=0;
  unsigned long free_blocks=0; 
  free_block *fake_head=NULL;
  
 public:
  static RetCode Open(const std::string& path, PageEngine** eptr);

  explicit DummyEngine(const std::string& path);

  ~DummyEngine() override;

  RetCode pageWrite(uint32_t page_no, const void *buf) override;


  RetCode pageRead(uint32_t page_no, void *buf) override;
  void insert_free_block(free_block *head,free_block *insert);
  bool pwriteByFreeBolck(size_t len,Bytef *compressBuf,int fd,uint32_t page_no);
  void mergeFree(uint32_t page_no);
};

#endif  // PAGE_ENGINE_DUMMY_ENGINE_H_
