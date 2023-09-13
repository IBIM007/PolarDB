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
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <vector>
#include <thread>

static const float SHUTDOWN_THD = 0.01;
static const int THREAD_NUM = 4;

typedef enum {
  IO_READ = 0,
  IO_WRITE = 1
} IOType;

struct Pipe {
  int pipefd[2];
};
//pipefd[0]是读端貌似

class Visitor {
 private:
  PageEngine *page_engine;
  const int page_size{16384};
  int thread_num;
  int pid;

  std::vector<Pipe> pipes;//和线程数量一样。下标和线程下标对应

 public:
  Visitor(int thread) : thread_num(thread), pid(-1) {
    for (int i=0; i<thread_num; i++) {
      pipes.emplace_back();//这个应该装入一个空对象吧。
      int rc = pipe(pipes.back().pipefd);//创建管道
      assert(rc != -1);
    }
  }

  ~Visitor() {
  }

  void thread_func(int thread_id) {
    assert(pid == 0);
    assert(page_size == 16384);
    //读端就是一个数字，不代表和线程的关系。
    std::cout << "Thread " << thread_id << " start on pipe " << pipes[thread_id].pipefd[0] << std::endl;
    void *page_ptr = malloc(2 * page_size);
    //~是按位取反。  0x3fff是16383，那么取反，其它再与操作，也就是只看这么多位。
    //buf和page_ptr区分开。buf应该是开始的意思。也就是在page_size/2 -1的位置吧。ptr是起始位置。
    void *page_buf = (void*)(((uint64_t)page_ptr + 16384) & (~0x3fff));

    void *trace_ptr = malloc(2 * page_size);
    void *trace_buf = (void*)(((uint64_t)trace_ptr + 16384) & (~0x3fff));

    while (true) {
      uint8_t cmd;
      uint32_t page_no;
      int bytes;
      RetCode ret;
      //命令是读还是写
      bytes = read(pipes[thread_id].pipefd[0], &cmd, 1);
      //为0的话这个线程就不作用了。
      if (bytes == 0) break;
      assert(bytes == 1);
      //页码
      bytes = read(pipes[thread_id].pipefd[0], &page_no, 4);
      assert(bytes == 4);
      //把，页的内容给读出来？，这个内容可能用作写，也可能用作读的核对。
      bytes = read(pipes[thread_id].pipefd[0], trace_buf, page_size);
      assert(bytes == bytes);

      switch(cmd) {
        case IO_READ:
          std::cout << "Thread " << thread_id << " Receive CMD: Read Page page_no: " << page_no << std::endl;
          //读出来给page_buf
          ret = page_engine->pageRead(page_no, page_buf);
          assert(ret == kSucc);
          //此时trace_buf是校验的东西。
          if(memcmp(page_buf, trace_buf, page_size) != 0){
            std::cout<<"page_buf是："<<(char*)page_buf<<std::endl;
            std::cout<<"trace_buf是："<<(char*)trace_buf<<std::endl;
            std::cout<<"这个页出错了："<<page_no<<std::endl;
          }
          assert(memcmp(page_buf, trace_buf, page_size) == 0);
          break;

        case IO_WRITE:
          std::cout << "Thread " << thread_id << " Receive CMD: Write Page page_no: " << page_no << std::endl;
          ret = page_engine->pageWrite(page_no, trace_buf);
          assert(ret == kSucc);
          break;
      }
    }

    free(page_ptr);
    free(trace_ptr);

    std::cout << "Thread " << thread_id << " exit" << std::endl;
  }
  //父进程调用的这个。
  void pageRead(uint32_t page_no, void* buf) {
    assert(pid > 0);
    //通过这种方式来选择让这个线程去读取这个页。分发的时候就固定好了线程和页的对应，确实不会存在多个页同时被操作的时候
    int thread_id = page_no % thread_num;
    uint8_t io_type = IO_READ;

    std::cout << "Send CMD to thread " << thread_id << ": Read Page page_no: " << page_no << std::endl;
    //往这个线程的写端写入东西，它就可以在读端读取到了。
    write(pipes[thread_id].pipefd[1], &io_type, 1);
    write(pipes[thread_id].pipefd[1], &page_no, 4);
    write(pipes[thread_id].pipefd[1], buf, page_size);
  }

  //同理
  void pageWrite(uint32_t page_no, void* buf) {
    assert(pid > 0);
    int thread_id = page_no % thread_num;
    uint8_t io_type = IO_WRITE;

    std::cout << "Send CMD to thread " << thread_id << ": Write Page page_no: " << page_no << std::endl;
    write(pipes[thread_id].pipefd[1], &io_type, 1);
    write(pipes[thread_id].pipefd[1], &page_no, 4);
    write(pipes[thread_id].pipefd[1], buf, page_size);
  }

  void run() {
    //创建一个子进程
    pid = fork();
    assert(pid >= 0);
    if (pid == 0) {//说明是子进程
      std::string path = "./";
      RetCode ret = PageEngine::Open(path, &page_engine);
      assert(ret == kSucc);

      std::vector<std::thread> threads;

      for (int thread_id=0; thread_id<thread_num; thread_id++) {
        //关闭线程的写端。
        close(pipes[thread_id].pipefd[1]);
        //然后跑起来thread_func
        threads.emplace_back(std::thread(&Visitor::thread_func, this, thread_id));
        sleep(1);
      }

      for (int thread_id=0; thread_id<thread_num; thread_id++) {
        threads[thread_id].join();
        close(pipes[thread_id].pipefd[0]);
      }


      // online judge will not call delete(page_engine)
      // delete(page_engine);
      //子进程终止了
      exit(0);
    } else {//父进程
      for (int thread_id=0; thread_id<thread_num; thread_id++) {
        close(pipes[thread_id].pipefd[0]);
      }
    }
  }

  void shutdown() {
    for (int thread_id=0; thread_id<thread_num; thread_id++) {
      close(pipes[thread_id].pipefd[1]);
    }

    int status;
    wait(&status);
    if (WIFEXITED(status)) {
      int exitCode = WEXITSTATUS(status);
      printf("Child process exited with code %d\n", exitCode);
      assert(exitCode == 0);
    } else {
      printf("Child process did not exit normally\n");
      assert(false);
    }
  }
};

void run_trace(std::string path) {
  std::ifstream trace_file(path);
  char RW;
  uint32_t page_no;
  const int page_size = 16384;

  Visitor *visitor = new Visitor(THREAD_NUM);
  visitor->run();

  //父进程还会返回回来继续执行。
  void *trace_buf = malloc(page_size);

  std::string line;
  while (std::getline(trace_file, line)) {
    std::stringstream linestream(line);
    if (!(linestream >> RW >> page_no)) break;
    trace_file.read((char*)trace_buf, page_size);

    switch(RW) {
      case 'R':
      {

        visitor->pageRead(page_no, trace_buf);
        break;
      }
      case 'W':
      {

        visitor->pageWrite(page_no, trace_buf);
        break;
      }
      default:
        assert(false);
    }
    //这是个啥意思。就是模拟宕机而已
    if ((float)rand() / RAND_MAX < SHUTDOWN_THD) {
      std::cout << "shutdown" << std::endl;
      visitor->shutdown();
      delete visitor;
      visitor = new Visitor(THREAD_NUM);
      visitor->run();
    }
  }
  trace_file.close();

  delete visitor;
  free(trace_buf);
}

int main(int argc, char* argv[]) {
  assert(argc == 2);

  std::string path(argv[1]);

  srand(0);

  run_trace(path);

  std::cout << "Finished trace run!" << std::endl;
  return 0;
}
