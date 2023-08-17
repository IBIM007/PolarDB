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

typedef struct Buffer_page
{
  unsigned long page_id;
  struct Buffer_page *next;
}buffer_page;

class DummyEngine : public PageEngine  {
 private:
  int fd;
  const size_t page_size{16384};
  //short size_map[655362];
  free_block *start[655362];//多个页一起压缩的话，那么要把多个start[i]设置为那个空闲块
  long long last_write=0;
  unsigned long free_blocks=0; 
  free_block *fake_head=NULL;

  buffer_page *pageid_start=NULL;
  free_block *buffer_start[655362];
  free_block *buffer_free_head=NULL;
  unsigned long pool_left_size;
  char *pool;

 public:
  static RetCode Open(const std::string& path, PageEngine** eptr);

  explicit DummyEngine(const std::string& path);

  ~DummyEngine() override;
//这是原版的
  RetCode pageWrite(uint32_t page_no, const void *buf) override;
//  RetCode pageWrite(uint32_t page_no, void *buf) override;

  RetCode pageRead(uint32_t page_no, void *buf) override;
  void insert_free_block(free_block *head,free_block *insert);
  bool pwriteByFreeBolck(size_t len,Bytef *compressBuf,int fd,uint32_t page_no,unsigned long &left,free_block **last);
  void mergeFree(uint32_t page_no);
  bool frePwriteByFreeBolck(size_t len,Bytef *compressBuf,int fd,uint32_t page_no);

  bool pwriteByFreeBolck_mem(size_t len,Bytef *compressBuf,int fd,uint32_t page_no,unsigned long &left,free_block **last);
  void insert_free_block_mem(free_block *head,free_block *insert);
};

#endif  // PAGE_ENGINE_DUMMY_ENGINE_H_
