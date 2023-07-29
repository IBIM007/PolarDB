// Copyright [2023] Alibaba Cloud All rights reserved
#include "dummy_engine.h"
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <iostream>
#include "zlib.h"
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
  std::cout<<data_file<<std::endl;
  fd = open(data_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  assert(fd >=0);
}

DummyEngine::~DummyEngine() {
  //std::cout<<"sum是："<<sum<<std::endl;
  if (fd >= 0) {
    close(fd);
  }
}
/*内存限制50MB（不含测评程序使用的内存）*/
//这是原版的
RetCode DummyEngine::pageWrite(uint32_t page_no, const void *buf) {
  uLong len=16384;//还真的必须用uLong这样来搞
  //直接压buf不行，因为如果buf塞满就没有'\0'了，所以过渡一下用original
  Bytef original[len+20];
  memset(original,'\0',len+20);
  memcpy(original,buf,len);

  uLong compressLen=compressBound(len+1);//现在要压缩的长度默认就是16385了，默认压缩最后自己追加的'\0'，需要这样+1。注意compressLen需要先定义好？
  Bytef compressBuf[compressLen];
  memset(compressBuf,'\0',compressLen);
  if(compress(compressBuf,&compressLen,(const Bytef *)original,16384+1)!=Z_OK)
  {
    std::cout<<"压缩出错了"<<std::endl;
  }

   size_map[page_no]=compressLen;
   ssize_t nwrite = pwrite(fd, compressBuf, compressLen, last_write);
   if (nwrite != compressLen) {
     return kIOError;
   }

   offset_map[page_no]=last_write;
   last_write+=compressLen;

  
  return kSucc;
}

RetCode DummyEngine::pageRead(uint32_t page_no, void *buf) {
  memset(buf,'\0',16384);
  ssize_t nwrite = pread(fd, buf, size_map[page_no], offset_map[page_no]);//这里有没有可能需要更大的buf来读取
  if (nwrite != size_map[page_no]) {
    return kIOError;
  }
  uLong compressLen=size_map[page_no];
  uLongf tlen=16384+1;//原本长度先定义好
  Bytef uncompressBuf[tlen];
  if(uncompress(uncompressBuf,&tlen,(const Bytef *)((char*)buf), compressLen)!=Z_OK)
  {
    //return kUncompressError;
  }
  memcpy(buf,uncompressBuf,16384);//它只比较前16384个字节
  
  return kSucc;
}
