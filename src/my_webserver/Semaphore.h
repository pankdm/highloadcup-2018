#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

class Semaphore {
   private:
    std::mutex mutex_;
    std::condition_variable condition_;
    std::atomic<int> count_ = 0;  // Initialized as locked.

   public:
    explicit Semaphore(int count) : count_(count) {}

    int getCounter() { return count_.load(); }

    void notify() {
        std::lock_guard<decltype(mutex_)> lock(mutex_);
        ++count_;
        condition_.notify_one();
    }

    void wait() {
        std::unique_lock<decltype(mutex_)> lock(mutex_);
        while (!count_)  // Handle spurious wake-ups.
            condition_.wait(lock);
        --count_;
    }

    bool try_wait() {
        std::lock_guard<decltype(mutex_)> lock(mutex_);
        if (count_) {
            --count_;
            return true;
        }
        return false;
    }
};

class ScopedSemaphore {
   public:
    explicit ScopedSemaphore(Semaphore& sem) : _semaphore(sem) { _semaphore.wait(); }
    ScopedSemaphore(const ScopedSemaphore&) = delete;
    ~ScopedSemaphore() { _semaphore.notify(); }

    ScopedSemaphore& operator=(const ScopedSemaphore&) = delete;

   private:
    Semaphore& _semaphore;
};
