// Copyright [2023] Alibaba Cloud All rights reserved
#include "dummy_engine.h"
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <iostream>
#include<malloc.h>

/*
 * Dummy sample of page engine
 */

RetCode PageEngine::Open(const std::string& path, PageEngine** eptr) {
  return DummyEngine::Open(path, eptr);
}

static std::string pathJoin(const std::string& p1, const std::string& p2) {
  char sep = '/';
  std::string tmp = p1;

#ifdef _WIN32
  sep = '\\';
#endif

  if (p1[p1.length() - 1] != sep) {
      tmp += sep;
      return tmp + p2;
  } else {
      return p1 + p2;
  }
}

RetCode DummyEngine::Open(const std::string& path, PageEngine** eptr) {
  DummyEngine *engine = new DummyEngine(path);
  *eptr = engine;
  return kSucc;
}

DummyEngine::DummyEngine(const std::string& path) {
  std::string data_file = pathJoin(path, "data.ibd");
  fd = open(data_file.c_str(), O_RDWR | O_CREAT|O_DIRECT, S_IRUSR | S_IWUSR);
  //fd = open(data_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  assert(fd >=0);
}

DummyEngine::~DummyEngine() {
  /*FILE* fp = fopen("/proc/self/status", "r");
    char line[128];
    while (fgets(line, 128, fp) != NULL)
    {
        if (strncmp(line, "VmRSS:", 6) == 0)
        {
            printf("当前进程占用内存大小为：%d KB\n", atoi(line + 6));
            break;
        }
    }
    fclose(fp);*/

  if (fd >= 0) {
    close(fd);
  }
}

RetCode DummyEngine::pageWrite(uint32_t page_no, const void *buf) {
  size_t compressSize;
  char *compressBuf;
  compressBuf= (char *) memalign(getpagesize(), 16384);
  //还可以调整到1这个级别，1级别不好，试试2效果，4,5,6,7都试试
  compressSize = ZSTD_compress(compressBuf, 16384, buf, 16384, 3);
  //std::cout<<compressSize<<std::endl;
  real_size_map[page_no]=compressSize;
  int much=4096;
  while(much<compressSize)
  {
    much+=4096;
  }
  size_map[page_no]=much;
  ssize_t nwrite = pwrite(fd, compressBuf, size_map[page_no], page_no * page_size);
  free(compressBuf);
  return kSucc;
}

RetCode DummyEngine::pageRead(uint32_t page_no, void *buf) {
  
  ssize_t nwrite;
  if(size_map[page_no]!=0)nwrite = pread(fd, buf, size_map[page_no], page_no * page_size);
  else nwrite = pread(fd, buf, 16384, page_no * page_size);
  unsigned long long decom_buf_size;
  if(real_size_map[page_no]!=0)decom_buf_size = ZSTD_getFrameContentSize(buf, real_size_map[page_no]);
  else decom_buf_size = ZSTD_getFrameContentSize(buf, 16384);
  char decom_ptr[decom_buf_size];
  size_t decom_size;
  //现在不知道这种按16384解压，是不是时间会很久，可以做一版size存磁盘。
  if(real_size_map[page_no]!=0)decom_size = ZSTD_decompress(decom_ptr, decom_buf_size, buf, real_size_map[page_no]);
  else decom_size = ZSTD_decompress(decom_ptr, decom_buf_size, buf, 16384);
  
 
  memcpy(buf,decom_ptr,16384);
  return kSucc;
}
