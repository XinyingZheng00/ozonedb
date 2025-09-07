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
  enum class Priority { High,
                        Low };

  ThreadPool(size_t numThreads);
  ~ThreadPool();
  void enqueue(std::function<void()> task, Priority priority = Priority::Low);
  void enqueueFetchOnceWithEndOffset(
      std::string const& file,
      size_t required_end_offset,
      std::function<void(size_t begin, size_t end)> fetch_func,  // you provide this
      std::function<void()> post_func);
  void waitForCompletion();  // Wait for all tasks to complete
  void stop();               // Graceful stop function

 private:
  std::vector<std::thread> workers;
  std::queue<std::function<void()>> highPriorityTasks;
  std::queue<std::function<void()>> lowPriorityTasks;
  std::mutex inflight_mu_;
  std::unordered_map<std::string, size_t> max_fetched_end_;
  std::unordered_map<std::string, std::vector<std::pair<size_t, std::function<void()>>>> waiting_post_funcs_;
  std::unordered_map<std::string, bool> fetch_in_progress_;

  std::mutex queueMutex;
  std::condition_variable condition;
  std::atomic<bool> stopFlag;
  std::mutex taskMutex;
  std::condition_variable taskCondition;
  std::atomic<size_t> activeTasks;
};

// geterpc() function -- get local erpc object

// Constructor: Create a pool of worker threads
inline ThreadPool::ThreadPool(size_t numThreads) : stopFlag(false), activeTasks(0) {
  for (size_t i = 0; i < numThreads; ++i) {
    workers.emplace_back([this]() {
      // maybe have erpc object for each worker thread
      while (true) {
        std::function<void()> task;
        {
          std::unique_lock<std::mutex> lock(this->queueMutex);
          this->condition.wait(lock, [this]() {
            return this->stopFlag || !this->highPriorityTasks.empty() || !this->lowPriorityTasks.empty();
          });

          if (this->stopFlag && this->highPriorityTasks.empty() && this->lowPriorityTasks.empty()) {
            return;
          }

          if (!this->highPriorityTasks.empty()) {
            task = std::move(this->highPriorityTasks.front());
            this->highPriorityTasks.pop();
          } else if (!this->lowPriorityTasks.empty()) {
            task = std::move(this->lowPriorityTasks.front());
            this->lowPriorityTasks.pop();
          }
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

inline void ThreadPool::enqueueFetchOnceWithEndOffset(
    std::string const& file,
    size_t required_end_offset,
    std::function<void(size_t begin, size_t end)> fetch_func,
    std::function<void()> post_func) {
  bool need_fetch = false;
  size_t begin_offset = 0;

  {
    std::unique_lock<std::mutex> lock(inflight_mu_);

    size_t current_max = max_fetched_end_[file];
    if (required_end_offset <= current_max) {
      this->enqueue(post_func, Priority::High);
      return;
    }

    // Not yet fully fetched — save post_func
    waiting_post_funcs_[file].emplace_back(required_end_offset, post_func);

    if (!fetch_in_progress_[file]) {
      fetch_in_progress_[file] = true;
      begin_offset = current_max;
      need_fetch = true;
      // std::cout << "thread id: " << std::this_thread::get_id() << " need_fetch: " << need_fetch << " file: " << file << " begin_offset: " << begin_offset << " required_end_offset: " << required_end_offset << std::endl;
    }
    else {
      // std::cout << "thread id: " << std::this_thread::get_id() << " skip fetch" << std::endl;
    }
  }

  if (need_fetch) {
    this->enqueue([this, file, begin_offset, required_end_offset, fetch_func]() {
      // Run the fetch task
      fetch_func(begin_offset, required_end_offset);

      std::vector<std::function<void()>> ready_callbacks;
      {
        std::unique_lock<std::mutex> lock(this->inflight_mu_);
        max_fetched_end_[file] = std::max(max_fetched_end_[file], required_end_offset);
        fetch_in_progress_[file] = false;

        // Drain all callbacks whose requirements are satisfied
        auto& pending = waiting_post_funcs_[file];
        auto it = pending.begin();
        while (it != pending.end()) {
          if (it->first <= max_fetched_end_[file]) {
            ready_callbacks.push_back(std::move(it->second));
            it = pending.erase(it);
          } else {
            ++it;
          }
        }

        if (pending.empty()) {
          waiting_post_funcs_.erase(file);
        }
      }

      for (auto& cb : ready_callbacks) {
        this->enqueue(cb, Priority::High);
      }
    },
                  Priority::High);
  }
}

// Destructor
inline ThreadPool::~ThreadPool() {
  stop();
}

// Enqueue a task with optional priority
inline void ThreadPool::enqueue(std::function<void()> task, Priority priority) {
  {
    std::unique_lock<std::mutex> lock(queueMutex);
    if (stopFlag) throw std::runtime_error("enqueue on stopped ThreadPool");

    if (priority == Priority::High) {
      highPriorityTasks.emplace(std::move(task));
    } else {
      lowPriorityTasks.emplace(std::move(task));
    }
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