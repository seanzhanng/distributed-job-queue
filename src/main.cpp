#include "queue/job.hpp"
#include "queue/job_queue.hpp"
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>

// atomic bool instead of regular bool so optimizer doesn't optimize this
std::atomic<bool> running{true};
const int MAX_JOB_ATTEMPTS = 3;
const int LEASE_DURATION = 2000;
const int WORKER_THREAD_COUNT = 50;

int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch())
        .count();
}

// keeps track of the time and also does the periodic check_expired sweep
void timer_thread(queue::JobQueue& queue) {
    while (running) {
        long long now = now_ms();

        // check_expired sweep
        queue.check_expired(now, MAX_JOB_ATTEMPTS);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void worker_thread(queue::JobQueue& queue, int worker_id) {
    while (running) {
        queue::Job job;
        long long now = now_ms();

        bool leased = queue.lease(now, LEASE_DURATION, job);

        if (leased) {
            // randomize each jobs working time
            int work_ms = 500 + std::rand() % 2000;

            // 20% chance the job errors out
            bool failed = (std::rand() % 5 == 0);

            std::cout << "Worker " << worker_id << " leased " << job.id
                      << " (epoch: " << job.lease_epoch << ", working for "
                      << work_ms << "ms)\n";

            // sleep to simulate doing work
            std::this_thread::sleep_for(std::chrono::milliseconds(work_ms));

            if (failed) {
                bool nacked =
                    queue.nack(job.id, job.lease_epoch, MAX_JOB_ATTEMPTS);
                std::cout << "Worker " << worker_id << " NACK for " << job.id
                          << ": "
                          << (nacked ? "REQUEUED/FAILED"
                                     : "REJECTED (fenced out)")
                          << "\n";
            } else {
                bool acked = queue.ack(job.id, job.lease_epoch);
                std::cout << "Worker " << worker_id << " ACK for " << job.id
                          << ": "
                          << (acked ? "ACCEPTED" : "REJECTED (fenced out)")
                          << "\n";
            }
        } else {
            // queue is currently empty, back off for 50ms before checking again
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
}

int main() {
    queue::JobQueue queue;

    // place jobs into the queue
    for (int i = 0; i < 1000; i++) {
        queue.enqueue("job-" + std::to_string(i));
    }

    std::cout << "all jobs enqueued!\n";

    // background timer thread
    std::thread timer(timer_thread, std::ref(queue));

    // worker thread pool
    std::vector<std::thread> workers;
    for (int i = 0; i < WORKER_THREAD_COUNT; i++) {
        workers.push_back(std::thread(worker_thread, std::ref(queue), i));
    }

    // pauses the main thread so worker threads have time to process jobs
    std::this_thread::sleep_for(std::chrono::seconds(10));

    // cleanup
    running = false;
    timer.join();
    for (int i = 0; i < WORKER_THREAD_COUNT; i++) {
        workers[i].join();
    }

    std::cout << "cleanup successful!\n";
    return 0;
}