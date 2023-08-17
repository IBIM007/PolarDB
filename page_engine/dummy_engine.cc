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
  //memset(size_map,-1,sizeof(size_map));
  for(int i=0;i<655362;++i)
  {
    start[i]=NULL;
  }

  for(int i=0;i<655362;++i)
  {
    buffer_start[i]=NULL;
  }

  pool=(char*)malloc(16384*100*sizeof(char));
  pool_left_size=16384*100;
  buffer_free_head=(free_block *)malloc(sizeof(free_block));
  buffer_free_head->offset=0;
  buffer_free_head->size=pool_left_size;
  std::cout<<"缓存池还剩余"<<pool_left_size<<std::endl;

}

DummyEngine::~DummyEngine() {
  if(buffer_free_head==NULL)
  {
    std::cout<<"内存空闲链表为空了"<<std::endl;
  }
   std::cout<<"缓存池剩余容量"<<pool_left_size<<std::endl;
  std::cout<<"最后的碎片是这么多个"<<free_blocks<<std::endl;
  unsigned long sum=0;
  while(fake_head)
  {
    sum+=fake_head->size;
    fake_head=fake_head->next;
  }
  std::cout<<"最后的碎片总大小是这么多"<<sum<<std::endl;
  if (fd >= 0) {
    close(fd);
  }
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

    while(fake_head)
    {
      free_block *next=fake_head;
      free(fake_head);
      fake_head=next;
    }

    fclose(fp);
}
//还是可以改成头插法，毕竟把原来的地方插入后剩余的内容已经不大了，pwrite要不了几次，头插法特快。感觉可能还是按照size排序比较好？
void DummyEngine::insert_free_block(free_block *head,free_block *insert)
{
  ++free_blocks;
  if(head==NULL)
  {
    insert->next=head;
    fake_head=insert;
    return ;
  }
  free_block *doit=head;
  free_block *doitpre=head;
  while(doit)
  {
    if(doit->size<=insert->size)break;

    if(doit!=head)doitpre=doit;
    doit=doit->next;

  }
  if(doit==head)
  {
    insert->next=head;
    fake_head=insert;
  }
  else{
    doitpre->next=insert;
      insert->next=doit;
  }
  
}

void DummyEngine::insert_free_block_mem(free_block *head,free_block *insert)
{
  ++free_blocks;
  if(head==NULL)
  {
    insert->next=head;
    buffer_free_head=insert;
    return ;
  }
  free_block *doit=head;
  free_block *doitpre=head;
  while(doit)
  {
    if(doit->size<=insert->size)break;

    if(doit!=head)doitpre=doit;
    doit=doit->next;

  }
  if(doit==head)
  {
    insert->next=head;
    buffer_free_head=insert;
  }
  else{
    doitpre->next=insert;
      insert->next=doit;
  }
  
}
//这里还要继续改
bool DummyEngine::pwriteByFreeBolck(size_t len,Bytef *compressBuf,int fd,uint32_t page_no,unsigned long &left,free_block **last)
{
  free_block *doit=fake_head;
  free_block *doitpre=fake_head;
  while(doit)
  {
    if(doit->size==left)
    {
      pwrite(fd,compressBuf+(len-left),left,doit->offset);
      if((*last)!=NULL)(*last)->next=doit;
      else 
      {
        *last=doit;
        start[page_no]=doit;
      }
      if(doit==doitpre)
      {
        fake_head=fake_head->next;
      }
      else
      {
        doitpre->next=doit->next;
      }
      doit->next=NULL;
      if((*last)->next!=NULL)(*last)=(*last)->next;
      
      left=0;
      return true;
    }
    else if(doit->size>left)
    {
      
      pwrite(fd,compressBuf+(len-left),left,doit->offset);
      if((*last)!=NULL)(*last)->next=doit;
      else 
      {
        *last=doit;
        start[page_no]=doit;
      }


      if(doit==doitpre)
      {
        fake_head=fake_head->next;
      }
      else
      {
        doitpre->next=doit->next;
      }
      doit->next=NULL;
      if((*last)->next!=NULL)(*last)=(*last)->next;

      free_block *free_block1=(free_block *)malloc(sizeof(free_block));
      free_block1->offset=doit->offset+left;
      free_block1->size=doit->size-left;
      insert_free_block(fake_head,free_block1);

      doit->size=left;
      left=0;
      return true;
    }
    else if(doit->size<left)
    {
      pwrite(fd,compressBuf+(len-left),doit->size,doit->offset);
    
      if((*last)!=NULL)(*last)->next=doit;
      else
      {
        *last=doit;
        start[page_no]=doit;
      } 


 
      if(doit==doitpre)
      {
        fake_head=fake_head->next;
      }
      else
      {
        doitpre->next=doit->next;
      }
      
      doit->next=NULL;
      if((*last)->next!=NULL)(*last)=(*last)->next;
      
      left-=doit->size;
      
      

    }
    if(doit!=fake_head)doitpre=doitpre->next;
    doit=doit->next;
    
  }
  return false;
}


//37页一来直接走的空闲块读取，没有设置start
RetCode DummyEngine::pageWrite(uint32_t page_no, const void *buf) {

  //跑了一分钟就G了
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
  //std::cout<<"压缩长度是"<<compressLen<<std::endl;
  //首先检查内存中是否有这个页。
  //如果有那么直接覆盖写，如果还有剩余，继续写入磁盘。
  //如果没有，那么直接通过内存空闲页进行写入，如果还有剩余，继续写入磁盘。
  unsigned long left_mem=compressLen;
  free_block *link_mem=buffer_start[page_no];
  free_block *last_mem=link_mem;
    while(link_mem!=NULL)
    {
    if(link_mem->size>left_mem)
    {
      //这里的compressBuf和compresslen要注意
      memcpy(pool+link_mem->offset,compressBuf+(compressLen-left_mem),left_mem);
      //剩余的部分也必须分出去。如果不分出去，要加入realsize
      free_block *free_block1=(free_block *)malloc(sizeof(free_block));
      free_block1->offset=link_mem->offset+left_mem;
      free_block1->size=link_mem->size-left_mem;
      insert_free_block_mem(buffer_free_head,free_block1);//暂定为buffer_free_head，后续再看是否符合逻辑
      pool_left_size+=free_block1->size;

      link_mem->size=left_mem;
      free_block *after=link_mem->next;
      while(after)
      {
        free_block *next=after->next;
        pool_left_size+=after->size;
        insert_free_block_mem(buffer_free_head,after);
        after=next;
      }
      left_mem=0;
      link_mem->next=NULL;
      break;
    }
    else if(link_mem->size==left_mem)
    {
      memcpy(pool+link_mem->offset,compressBuf+(compressLen-left_mem),left_mem);
      free_block *after=link_mem->next;
      while(after)
      {
        free_block *next=after->next;
        pool_left_size+=after->size;
        insert_free_block_mem(buffer_free_head,after);
        after=next;
      }
      left_mem=0;
      link_mem->next=NULL;
      break;
    }
    if(link_mem->size<left_mem)
    {
      memcpy(pool+link_mem->offset,compressBuf+(compressLen-left_mem),link_mem->size);
      left_mem-=link_mem->size;
    }
    link_mem=link_mem->next;
    if(link_mem!=NULL)last_mem=link_mem;//如果link变成空，last则是最后一个块。并且left!=0，那么last刚好可以继续链接。
    }

    //原来的位置不够写入。继续内存空闲块写入。
    bool pwriteByFree_mem=false;
    if(left_mem==0)return kSucc;
  if(left_mem!=0)
  {
   
    pwriteByFree_mem=pwriteByFreeBolck_mem(left_mem,compressBuf+(compressLen-left_mem),fd,page_no,left_mem,&last_mem);
  }
  if(pwriteByFree_mem)return kSucc;
  //并没有淘汰任何东西。

  
  //这里执行完了，compressBuf应该继续加

  //size_map[page_no]=compressLen;
  bool pwriteByFree=false;
  //unsigned long left=compressLen;//还剩余多少没写完
  unsigned long left=left_mem;
  //说明之前已经压缩存进去过了,只有这种情况才会开始产生空洞。不管是否大于，都可以变成空闲块，然后统一用空闲块来写，代码会很简洁。
  free_block *link=start[page_no];
  free_block *last=link;//第一次写的话，last就是为空，那么第一次写还如果用空闲块写的话，那么last就没值了
  //1通过原来的块来先写一部分
  while(link!=NULL)
  {
    
    if(link->size>left)
    {
      //这里的compressBuf和compresslen要注意
      pwrite(fd,compressBuf+(compressLen-left),left,link->offset);
      //剩余的部分也必须分出去。如果不分出去，要加入realsize
      free_block *free_block1=(free_block *)malloc(sizeof(free_block));
      free_block1->offset=link->offset+left;
      free_block1->size=link->size-left;
      insert_free_block(fake_head,free_block1);//改成头插法

      link->size=left;
      free_block *after=link->next;
      while(after)
      {
        free_block *next=after->next;
        
        insert_free_block(fake_head,after);
        after=next;
      }
      left=0;
      link->next=NULL;
      break;
    }
    else if(link->size==left)
    {
      pwrite(fd,compressBuf+(compressLen-left),left,link->offset);
      free_block *after=link->next;
      while(after)
      {
        free_block *next=after->next;
        
        insert_free_block(fake_head,after);
        after=next;
      }
      left=0;
      link->next=NULL;
      //后面的块全部变成空闲块吗？算了吧，还是留给原来的当做局部性，不行必须删了，不然读取的时候没法控制
      break;
    }
    if(link->size<left)
    {
      pwrite(fd,compressBuf+(compressLen-left),link->size,link->offset);
      left-=link->size;
      
      //还有后续操作吗？
    }
    link=link->next;
    if(link!=NULL)last=link;//如果link变成空，last则是最后一个块。并且left!=0，那么last刚好可以继续链接。
    
  }
  //如果没有写完，还需要把pwrite里面把空闲块链接到start里面。
  //还没写完，2然后通过空闲块再写一部分
  if(left!=0)
  {
    pwriteByFree=pwriteByFreeBolck(left,compressBuf+(compressLen-left),fd,page_no,left,&last);
  }


  //还没写完，就追加写最后一部分。
  if(!pwriteByFree&&left!=0)
  { 
    //追加写不要就分配多余的空间，多次访问才分配多余的空间
    //如果是走了上面的原来存在页，那么追加写也多给200嘛。
    //这后面肯定还要改，现在只是按最简单情况处理的
    
    ssize_t nwrite = pwrite(fd, compressBuf+(compressLen-left), left, last_write);
    if (nwrite != left) {
     return kIOError;
    }
    free_block *used=(free_block *)malloc(sizeof(free_block));
    used->offset=last_write;
    used->size=left;
    used->next=NULL;
    if(last!=NULL)
    {
      last->next=used;//这里有问题，last其实不一定有值
    }
    else{
      
      start[page_no]=used;
    }
    last_write+=left;
  }
  //std::cout<<"写结束"<<std::endl;
 
  return kSucc;
}
//32和42页重叠了。
RetCode DummyEngine::pageRead(uint32_t page_no, void *buf) {
  memset(buf,'\0',16384);
  free_block *link=start[page_no];
  unsigned long already_read=0;
  //先从内存读取
  free_block *link_mem=buffer_start[page_no];
  bool print=false;
  while(link_mem)
  {
    memcpy(buf+already_read,pool+link_mem->offset,link_mem->size);
    already_read+=link_mem->size;
    link_mem=link_mem->next;
  }

  //再从磁盘读取
  print=false;
  
  while(link)
  {
    
    pread(fd,buf+already_read,link->size,link->offset);
    already_read+=link->size;
    link=link->next;
  }
  //说明有人在后面把32页的磁盘位置给写了。
  //std::cout<<(char*)buf<<std::endl;
  //ssize_t nwrite = pread(fd, buf, size_map[page_no], offset_map[page_no]);//这里有没有可能需要更大的buf来读取
  //if (nwrite != size_map[page_no]) {
  //  return kIOError;
  //}
  uLongf tlen=16384+1;//原本长度先定义好
  Bytef uncompressBuf[tlen];
  if(uncompress(uncompressBuf,&tlen,(const Bytef *)((char*)buf), already_read)!=Z_OK)
  {
    //return kUncompressError;
  }
  memcpy(buf,uncompressBuf,16384);//它只比较前16384个字节
  //std::cout<<(char*)buf<<std::endl;
  return kSucc;
}



bool DummyEngine::pwriteByFreeBolck_mem(size_t len,Bytef *compressBuf,int fd,uint32_t page_no,unsigned long &left,free_block **last)
{
  free_block *doit=buffer_free_head;
  free_block *doitpre=buffer_free_head;
  //std::cout<<2<<std::endl;
  while(doit)
  {
     
    if(doit->size==left)
    {
       pool_left_size-=doit->size;
      memcpy(pool+doit->offset,compressBuf+(len-left),left);
      if((*last)!=NULL)(*last)->next=doit;
      else 
      {
        *last=doit;
        buffer_start[page_no]=doit;
      }
      if(doit==doitpre)
      {
        buffer_free_head=buffer_free_head->next;
      }
      else
      {
        doitpre->next=doit->next;
      }
      doit->next=NULL;
      if((*last)->next!=NULL)(*last)=(*last)->next;
      
      left=0;
      return true;
    }
    else if(doit->size>left)
    {
      pool_left_size-=left;
      memcpy(pool+doit->offset,compressBuf+(len-left),left);
      //std::cout<<4<<std::endl;
      if((*last)!=NULL)(*last)->next=doit;
      else 
      {
        *last=doit;
        buffer_start[page_no]=doit;
      }
      //std::cout<<5<<std::endl;
      if(doit==doitpre)
      {
        buffer_free_head=buffer_free_head->next;
      }
      else
      {
        doitpre->next=doit->next;
      }
     
      doit->next=NULL;
      if((*last)->next!=NULL)(*last)=(*last)->next;
      
      free_block *free_block1=(free_block *)malloc(sizeof(free_block));
      free_block1->offset=doit->offset+left;
      free_block1->size=doit->size-left;
      
      insert_free_block_mem(buffer_free_head,free_block1);
     
      doit->size=left;
      left=0;
     
      return true;
    }
    else if(doit->size<left)
    {
      pool_left_size-=doit->size;
      memcpy(pool+doit->offset,compressBuf+(len-left),doit->size);
      if((*last)!=NULL)(*last)->next=doit;
      else
      {
        *last=doit;
        buffer_start[page_no]=doit;
      } 
      if(doit==doitpre)
      {
        buffer_free_head=buffer_free_head->next;
      }
      else
      {
        doitpre->next=doit->next;
      }
      doit->next=NULL;
      if((*last)->next!=NULL)(*last)=(*last)->next;
      left-=doit->size;
    }
    if(doit!=fake_head)doitpre=doitpre->next;
    doit=doit->next;
  }
  return false;
}