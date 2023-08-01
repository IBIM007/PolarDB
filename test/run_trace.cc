// Copyright [2023] Alibaba Cloud All rights reserved

/*
 * Local test to run sample trace
 */

#include "page_engine.h"
#include <iostream>
#include <fstream>
#include <cassert>
#include <cstring>
#include <sstream>
#include "../page_engine/zlib.h"
/*分工，研究一下这个trace*/
class Visitor {
 private:
  PageEngine *page_engine;
  const int page_size{16384};
  void *page_buf;
  void *trace_buf;

 public:
  Visitor() {
    std::string path = "./";
    RetCode ret = PageEngine::Open(path, &page_engine);
    assert(ret == kSucc);

    page_buf = malloc(page_size);/*页缓存*/
    trace_buf = malloc(page_size);/*多了一个trace buffer*/
  }

  ~Visitor() {
    delete page_engine;
    free(page_buf);
    free(trace_buf);
  }

  void run_trace(std::string path) {
    std::ifstream trace_file(path);/*流，用了路径的*/
    char RW;
    uint32_t page_no;

    std::string line;
    while (std::getline(trace_file, line)) {/*这是把输入的line写入到流里面吗？只读1行*/
      std::stringstream linestream(line);/*这个R和W应该是测试用例里面的吧.对的测试用例里面有1行W 320这种*/
      if (!(linestream >> RW >> page_no)) break;//写入是读还是写，把页号写入page_no
      trace_file.read((char*)trace_buf, page_size);/*这里是从下一行开始读取，读取16KB的数据。如果前面是W，那么接下来就是直接写这个trace_buf，如果前面是R，现在的trace_buf就是原本正确数据，那么通过读取解压.ibd放入page_buf与tracebuf比较即可。
      读取，跟上面可以想象成一句。*/

      switch(RW) {
        case 'R':
        {
          std::cout << "Read Page page_no: " << page_no << std::endl;
          RetCode ret = page_engine->pageRead(page_no, page_buf);/*这里读取时读出来到page_buf中。*/
          //std::cout<<"trace_buf读出来是："<<(char*)trace_buf<<std::endl;
          //std::cout<<"page_buf读出来是："<<(char*)page_buf<<std::endl;
          assert(ret == kSucc);
          assert(memcmp(page_buf, trace_buf, page_size) == 0);/*其功能是把存储区 str1 和存储区 str2 的前 n 个字节进行比较。该函数是按字节比较的，位于string.h*/
          break;
        }
        case 'W':
        {
          std::cout << "Write Page page_no: " << page_no << std::endl;
          RetCode ret = page_engine->pageWrite(page_no, trace_buf);/*通过trace_buf来写*/
          assert(ret == kSucc);/*应该都是先压缩写进磁盘的./ibd文件吧，可能trace测试用例里面全是W下面才有数据？*/
          break;
        }
        default:
          assert(false);
      }
    }
    trace_file.close();
  }
};

int main(int argc, char* argv[]) {
  
  /*std::string s;
  uLong len=16384/2;
  for(int i=0;i<len;i++)
  {
    s.push_back('1');
    s.push_back('\0');
  }
  std::cout<<"s的size是"<<s.size()<<std::endl;
  
  //std::cout<<oribuf<<std::endl;
  char compressBuf[2*len];
  //memset(compressBuf,'\0',50);
  uLong compressLen;
  compress((Bytef *)compressBuf,&compressLen,(const Bytef *)s.c_str(),16384);

  std::cout<<"压缩后的长度是"<<compressLen<<std::endl;
  std::cout<<"压缩后的字符串"<<compressBuf<<std::endl;
  char uncompressBuf[len*2];
    uLong uncompressLen; 
    uncompress ((Bytef *)uncompressBuf,&uncompressLen,(const Bytef *)compressBuf, compressLen);
    std::cout<<"解压后的长度是"<<uncompressLen<<std::endl;

    std::cout<<"解压后的字符串"<<uncompressBuf<<std::endl;
  */
  assert(argc == 2);
  /*传入的参数是路径,这个传输的应该就是另一个git的测试用例*/
  std::string path(argv[1]);

  Visitor visitor = Visitor();

  visitor.run_trace(path);

  std::cout << "Finished trace run!" << std::endl;
  return 0;
}
