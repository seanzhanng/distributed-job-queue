#include "queue/job.hpp"
#include "queue/job_queue.hpp"
#include <chrono>
#include <iostream>

int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch())
        .count();
}

int main() {
    JobQueue queue;

    // place jobs into the queue
    for (int i = 0; i < 10000; i++) {
        queue.enqueue("job-" + std::to_string(i));
    }

    std::cout << "all jobs enqueued!\n";
    return 0;
}