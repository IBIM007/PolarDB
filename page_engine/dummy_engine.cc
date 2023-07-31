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
  memset(size_map,-1,sizeof(size_map));
  //memset(offset_map,-1,sizeof(offset_map));
}

DummyEngine::~DummyEngine() {
  std::cout<<"碎片有这么多："<<free_blocks<<std::endl;
  FILE* fp = fopen("/proc/self/status", "r");
    char line[128];
    while (fgets(line, 128, fp) != NULL)
    {
        if (strncmp(line, "VmRSS:", 6) == 0)
        {
            printf("当前进程占用内存大小为：%d KB\n", atoi(line + 6));
            break;
        }
    }
    fclose(fp);
  if (fd >= 0) {
    close(fd);
  }
}

void DummyEngine::insert_free_block(free_block *head,free_block *insert)
{
  if(head==NULL)
  {
    insert->next=head;
    fake_head=insert;
  }
  else{
    free_block *doit=head;
    free_block *doitpre=head;
    while(doit)
    {
      if(doit->offset>insert->offset)//相等的情况思考一下会不会出现
      {
          //insert->next=doit;
          break;
      }
      else
      {
        if(doit!=head)doitpre=doitpre->next;
        doit=doit->next;
      }
    }
    if(doit==head)
    {
      insert->next=doit;
      fake_head=insert;//这个要注意，是改变fake_head的值，改变head值没用
    }
    else
    {
      doitpre->next=insert;
      insert->next=doit;
    }
  }
  ++free_blocks;
}
//TODO
void DummyEngine::mergeFree(uint32_t page_no)
{
  if(fake_head==NULL||fake_head->next==NULL)return ;

  free_block *doit=fake_head;
  free_block *doitpre=fake_head;
  while(doit->next)
  {
    if((doit->offset+doit->size)>=doit->next->offset)
    {
        doit->size+=doit->next->size;
        free_block *discard=doit->next;
        doit->next=doit->next->next;
        free(discard);
        --free_blocks;
    }
    doit=doit->next;
    if(doit==NULL)break;
  }
}
bool DummyEngine::pwriteByFreeBolck(size_t len,Bytef *compressBuf,int fd,uint32_t page_no)
{
  //找空闲块时实际上可以用不同的算法，1找刚好能装下的，2.找第一个能装下的立刻装了返回，3.找最大的size
  free_block *doit=fake_head;
  free_block *doitpre=fake_head;
  //现在做简单点，找第一个能装下的立刻返回。
  while(doit)
  {
    if(doit->size>=len)break;
    if(doit!=fake_head)doitpre=doitpre->next;
    doit=doit->next;
  }
  if(doit==NULL)return false;

  offset_map[page_no]=doit->offset;
  size_map[page_no]=len;

  pwrite(fd,compressBuf,len,doit->offset);
  doit->offset+=len;
  doit->size=doit->size-len;
  //如果为0还是需要清除掉。清除0，还需要注意头结点问题哦。
  if(doit->size==0)
  {
    if(doit==fake_head)
    {
      fake_head=fake_head->next;
      free(doit);
    }
    else{
      doitpre->next=doit->next;
      free(doit);
    }
    --free_blocks;
    //减少空闲块数
  }
  return true;
}

RetCode DummyEngine::pageWrite(uint32_t page_no, const void *buf) {
  uLong len=16384;//还真的必须用uLong这样来搞
  //直接压buf不行，因为如果buf塞满就没有'\0'了，所以过渡一下用original
  Bytef original[len+2];
  memset(original,'\0',len+2);
  memcpy(original,buf,len);

  uLong compressLen=compressBound(len+1);//现在要压缩的长度默认就是16385了，默认压缩最后自己追加的'\0'，需要这样+1。注意compressLen需要先定义好？
  Bytef compressBuf[compressLen];
  memset(compressBuf,'\0',compressLen);
  if(compress(compressBuf,&compressLen,(const Bytef *)original,16384+1)!=Z_OK)
  {
    std::cout<<"压缩出错了"<<std::endl;
  }
  
  bool pwriteByFree=false;
  bool merge=true;
  //说明之前已经压缩存进去过了,只有这种情况才会开始产生空洞。不管是否大于，都可以变成空闲块，然后统一用空闲块来写，代码会很简洁。
  if(size_map[page_no]!=-1)
  {
    //std::cout<<1<<std::endl;
    //原来的地方能够装下来，那么直接在原来的地方写入，实际上都可以做成统一的空闲块写入，不用这么区分
    if(size_map[page_no]>=compressLen)
    {
      //std::cout<<2<<std::endl;
      unsigned long originalSize=size_map[page_no];//是否有必要清0，如果不清0的话，后面读对应的字节也可以呀，不用物理清0吧。
      char empty[originalSize];
      memset(empty,'\0',originalSize);
      pwrite(fd,empty,originalSize,offset_map[page_no]);//两次系统调用，单纯追加写不会两次系统调用

      size_map[page_no]=compressLen;
      pwrite(fd,compressBuf,compressLen,offset_map[page_no]);
      
      //转入后看是否有剩余，&&free_blocks<500000
      if(size_map[page_no]!=compressLen)
      {
        //大小不同才会有空闲块。
        free_block* new_block=(free_block *)malloc(sizeof(free_block));
        new_block->offset=offset_map[page_no]+compressLen;
        new_block->size=size_map[page_no]-compressLen;
        insert_free_block(fake_head,new_block);
        //mergeFree(page_no);
      }
      //last最后的追加写位置不变
      return kSucc;
    }
    else
    {
      //std::cout<<3<<std::endl;
      //原来存在但是放不下。那么首先把原来的清空，然后放入空闲块。
      unsigned long originalSize=size_map[page_no];
      char empty[originalSize];
      memset(empty,'\0',originalSize);
      pwrite(fd,empty,originalSize,offset_map[page_no]);
      free_block* new_block=(free_block *)malloc(sizeof(free_block));
      new_block->offset=offset_map[page_no];
      new_block->size=size_map[page_no];
      insert_free_block(fake_head,new_block);

      //接着查看是否能通过空闲块来写入
      pwriteByFree=pwriteByFreeBolck(compressLen,compressBuf,fd,page_no);
    }
  }
  //原来并没有，那么首先查看是否能通过空闲块写入。
  else
  {
    //std::cout<<4<<std::endl;
    pwriteByFree=pwriteByFreeBolck(compressLen,compressBuf,fd,page_no);
  }


  //如果不能通过空闲块写入，那么直接追加写
  if(!pwriteByFree)
  {
    //std::cout<<5<<std::endl;
    size_map[page_no]=compressLen;
    ssize_t nwrite = pwrite(fd, compressBuf, compressLen, last_write);
    if (nwrite != compressLen) {
     return kIOError;
    }

    offset_map[page_no]=last_write;
    last_write+=compressLen;
    merge=false;
  }

  //最后合并空闲区。
  if(merge){
    //std::cout<<6<<std::endl;
    mergeFree(page_no);
  }
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
