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
}

DummyEngine::~DummyEngine() {
  if (fd >= 0) {
    close(fd);
  }
}

void DummyEngine::insert_free_block(free_block *head,free_block *insert)
{
  ++free_blocks;
  if(head==NULL)
  {
    insert->next=head;
    fake_head=insert;
  }
  else{
    free_block *doit=head;
    free_block *doitpre=head;
    free_block *doitprepre=head;//新加的
    while(doit)
    {
      if(doit->offset>insert->offset)//相等的情况思考一下会不会出现，应该不可能
      {
          break;
      }
      else
      {
        if(doitpre!=head)doitprepre=doitprepre->next;//新加的
        if(doit!=head)doitpre=doitpre->next;
        doit=doit->next;
      }
    }
    if(doit==head)//第一个的offset都比insert大
    {
      insert->next=doit;
      fake_head=insert;//这个要注意，是改变fake_head的值，改变head值没用
      //这种情况只需要考虑Insert是否能往后合并。
      if(insert->offset+insert->size>=doit->offset)
      {
        --free_blocks;
        free_block *newhead=(free_block *)malloc(sizeof(free_block));
        newhead->offset=insert->offset;
        newhead->size=insert->size+doit->size;
        newhead->next=doit->next;  //next指针
        fake_head=newhead;
        free(insert);
        free(doit);
      }
    }
    else
    { 
      //doit可能为空，
      //一个结点时，doit就是空了，此时doitpre和doitprepre都是head。
      //两个结点时，doit就是第二个，或者是空
      //三个节点时，
      doitpre->next=insert;
      insert->next=doit;
      //这种情况较为复杂。几种情况。
      //如果prepre==pre，说明doit要么为空，要么就是第二个

      //此时两者都是头结点
      if(doitprepre==doitpre)
      {
        if(doit==NULL)//此时布局，doitpre，insert，null
        {
          if(doitpre->offset+doitpre->size>=insert->offset)
          {
            //只差这种情况没有测过了，应该也没问题，测过了
            --free_blocks;
            free_block *newhead=(free_block *)malloc(sizeof(free_block));
            newhead->offset=doitpre->offset;
            newhead->size=doitpre->size+insert->size;
            newhead->next=doit;
            fake_head=newhead;
            free(insert);
            free(doitpre);
          }
          
        }
        else///此时布局，doitpre(还是头)，insert，doit ....
        {
          bool yes=false;
          if((doitpre->offset+doitpre->size)>=insert->offset)
          {
            --free_blocks;
            free_block *newhead=(free_block *)malloc(sizeof(free_block));
            newhead->offset=doitpre->offset;
            newhead->size=doitpre->size+insert->size;
            newhead->next=doit;
            fake_head=newhead;
            free(insert);
            free(doitpre);
            yes=true;
          }
          if(yes)//此时布局，insert和doitpre不存在。fake_head,doit....
          {
            doitpre=fake_head;
            insert=fake_head;
          }
          //没进入上面这个，那也是doitpre,insert,doit。。。来判断insert和doit
          if((insert->offset+insert->size)>=doit->offset)
          {
            --free_blocks;
            free_block *newhead=(free_block *)malloc(sizeof(free_block));
            newhead->offset=insert->offset;
            newhead->size=insert->size+doit->size;
            newhead->next=doit->next;
            if(doitpre==insert)//此时就是head,doit这种情况。并且已经合并成一个节点了
            {
              fake_head=newhead;
              free(insert);
              free(doit);
            }
            else
            {
              doitpre->next=newhead;
              free(insert);
              free(doit);
            }
          }
          //此时doit一定是第二个节点，就是考虑1，insert,2这三个节点合并。
        }
      }
      else//此时布局,doitprepre,doitpre,insert,doit，doit仍然可以为空
      {
        bool yesdo=false;
        if(doitpre->offset+doitpre->size>=insert->offset)
        {
          --free_blocks;
          free_block *newhead=(free_block *)malloc(sizeof(free_block));
            newhead->offset=doitpre->offset;
            newhead->size=doitpre->size+insert->size;
            newhead->next=doit;
            doitprepre->next=newhead;
            free(doitpre);
            free(insert);
            yesdo=true;
        }
        //现在不知道前面合成功了还是失败了
        if(yesdo)
        {
          insert=doitprepre->next;
          doitpre=doitprepre;
        }
        //如果成功了，那么布局就是doitprepre,insert,doit。如果没成功doitprepre,doitpre,insert,doit。始终是想看insert和doit
        if(doit!=NULL)
        {
            //此时布局是doitprepre insert doit ...
            if(insert->offset+insert->size>=doit->offset)
            {
              --free_blocks;
              free_block *newhead=(free_block *)malloc(sizeof(free_block));
              newhead->offset=insert->offset;
              newhead->size=insert->size+doit->size;
              newhead->next=doit->next;
              doitpre->next=newhead;
              free(insert);
              free(doit);
            }
        }
        //布局就是doitprepre doitpre insert doit（可能是空也可能是节点）。合并只能是doitpre和insert和doit这几个之间
      }
    }
  }
}

bool DummyEngine::pwriteByFreeBlock(size_t len,Bytef *compressBuf,int fd,uint32_t page_no)
{
  //找空闲块时实际上可以用不同的算法，1找刚好能装下的，2.找第一个能装下的立刻装了返回，3.找最大的size
  free_block *doit=fake_head;
  if(doit==NULL)return false;
  free_block *doitpre=fake_head;
  //现在做简单点，找第一个能装下的立刻返回。
  //这种方式0.26
  while(doit)
  {
    if(doit->size>=len)break;
    if(doit!=fake_head)doitpre=doitpre->next;
    doit=doit->next;
  }
  if(doit==NULL)return false;

  //接下来做找最大块。感觉这种性能最差，时间还可能超了
  /*size_t maxlen=doit->size;
  free_block *max=doit;
  free_block *maxpre=doitpre;
  while(doit)
  {
    if(doit->size>maxlen)
    {
      max=doit;
      maxpre=doitpre;
    }
    if(doit!=fake_head)doitpre=doitpre->next;
    doit=doit->next;
  }
  //if(max==NULL)return false;
  if(max->size<len)return false;
  doit=max;
  doitpre=maxpre;*/

  //接下来找刚好能装下的一个块，感觉应该是效果最好的
  /*free_block *perfect=doit;
  free_block *perfectpre=doitpre;
  while(doit)
  {
    if(doit->size>len&&doit->size<=perfect->size)
    {
      perfect=doit;
      perfectpre=doitpre;
    }
    if(doit!=fake_head)doitpre=doitpre->next;
    doit=doit->next;
  }
  if(perfect->size<len)return false;
  doit=perfect;
  doitpre=perfectpre;*/

  
  offset_map[page_no]=doit->offset;
  size_map[page_no]=len;

  pwrite(fd,compressBuf,len,doit->offset);
  doit->offset+=len;
  doit->size=doit->size-len;

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
  //std::cout<<"此页压缩后的长度为："<<compressLen<<std::endl;
  bool pwriteByFree=false;
  bool merge=true;
  //说明之前已经压缩存进去过了,只有这种情况才会开始产生空洞。不管是否大于，都可以变成空闲块，然后统一用空闲块来写，代码会很简洁。
  if(size_map[page_no]!=-1)
  {
    //超时严重还是只能通过这里如果发现长度大于等于那么，直接写入。再插入空闲区。
    bool zero=false;
    if(size_map[page_no]>=compressLen)
    {
      if(size_map[page_no]==compressLen)zero=true;
      pwrite(fd,compressBuf,compressLen,offset_map[page_no]);
      size_map[page_no]=compressLen;
      if(!zero)
      {
        free_block* new_block=(free_block *)malloc(sizeof(free_block));
        new_block->offset=offset_map[page_no]+compressLen;
        new_block->size=size_map[page_no]-compressLen;
        insert_free_block(fake_head,new_block);
      }
      return kSucc;
    }

    free_block* new_block=(free_block *)malloc(sizeof(free_block));
      new_block->offset=offset_map[page_no];
      new_block->size=size_map[page_no];
      insert_free_block(fake_head,new_block);//只有这里面才能出现合并吧。加速可以传一个参数进去，如果是空，那么就去找，如果不是空，那么直接插入并且返回结束。
  }
  pwriteByFree=pwriteByFreeBlock(compressLen,compressBuf,fd,page_no);

  //如果不能通过空闲块写入，那么直接追加写
  if(!pwriteByFree)
  {
    size_map[page_no]=compressLen;
    ssize_t nwrite = pwrite(fd, compressBuf, compressLen, last_write);
    if (nwrite != compressLen) {
     return kIOError;
    }

    offset_map[page_no]=last_write;
    last_write+=compressLen;
    merge=false;
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
