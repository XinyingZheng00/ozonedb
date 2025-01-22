#ifndef OZONEDB_THREAD_POOL_H
#define OZONEDB_THREAD_POOL_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

// Thread Pool class with task completion tracking
class ThreadPool {
 public:
  ThreadPool(size_t numThreads);
  ~ThreadPool();
  void enqueue(std::function<void()> task);
  void waitForCompletion();  // Wait for all tasks to complete
  void stop();               // Graceful stop function

 private:
  std::vector<std::thread> workers;
  std::queue<std::function<void()>> tasks;
  std::mutex queueMutex;
  std::condition_variable condition;
  std::atomic<bool> stopFlag;
  std::mutex taskMutex;
  std::condition_variable taskCondition;
  std::atomic<size_t> activeTasks;  // Counter for active tasks
};

// Constructor: Create a pool of worker threads
inline ThreadPool::ThreadPool(size_t numThreads) : stopFlag(false), activeTasks(0) {
  for (size_t i = 0; i < numThreads; ++i) {
    workers.emplace_back([this]() {
      while (true) {
        std::function<void()> task;
        {
          std::unique_lock<std::mutex> lock(this->queueMutex);
          this->condition.wait(lock, [this]() { return this->stopFlag || !this->tasks.empty(); });
          if (this->stopFlag && this->tasks.empty()) return;
          task = std::move(this->tasks.front());
          this->tasks.pop();
        }
        {
          std::unique_lock<std::mutex> lock(this->taskMutex);
          ++this->activeTasks;
        }
        task();
        {
          std::unique_lock<std::mutex> lock(this->taskMutex);
          if (--this->activeTasks == 0) {
            this->taskCondition.notify_all();
          }
        }
      }
    });
  }
}

// Destructor: Stop all threads and clean up
inline ThreadPool::~ThreadPool() {
  stop();
}

// Add a new task to the queue
inline void ThreadPool::enqueue(std::function<void()> task) {
  {
    std::unique_lock<std::mutex> lock(queueMutex);
    if (stopFlag) throw std::runtime_error("enqueue on stopped ThreadPool");
    tasks.emplace(std::move(task));
  }
  condition.notify_one();
}

// Wait for all tasks to complete
inline void ThreadPool::waitForCompletion() {
  std::unique_lock<std::mutex> lock(taskMutex);
  taskCondition.wait(lock, [this]() { return this->activeTasks == 0; });
}

// Graceful stop function
inline void ThreadPool::stop() {
  {
    std::unique_lock<std::mutex> lock(queueMutex);
    stopFlag = true;
  }
  condition.notify_all();
  for (std::thread& worker : workers) {
    if (worker.joinable()) {
      worker.join();
    }
  }
}

#endif  // OZONEDB_THREAD_POOL_H