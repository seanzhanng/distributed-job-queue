#include "queue/job.hpp"
#include "queue/job_queue.hpp"
#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

// atomic bool instead of regular bool so optimizer doesn't optimize this
std::atomic<bool> running{true};
const int MAX_JOB_ATTEMPTS = 3;

int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch())
        .count();
}

// keeps track of the time and also does the periodic check_expired sweep
void timer_thread(JobQueue& queue) {
    while (running) {
        long long now = now_ms();

        // check_expired sweep
        queue.check_expired(now, MAX_JOB_ATTEMPTS);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

int main() {
    JobQueue queue;

    // place jobs into the queue
    for (int i = 0; i < 10000; i++) {
        queue.enqueue("job-" + std::to_string(i));
    }

    std::cout << "all jobs enqueued!\n";

    // background timer thread
    std::thread timer(timer_thread, std::ref(queue));

    // cleanup
    running = false;
    timer.join();

    std::cout << "cleanup successful!\n";
    return 0;
}