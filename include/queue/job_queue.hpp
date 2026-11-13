#pragma once

#include "queue/job.hpp"
#include <mutex>
#include <string>
#include <vector>

class JobQueue {
  private:
    std::vector<Job> job_queue;
    std::mutex mtx;

  public:
    // adds a new job to the job queue
    void enqueue(const std::string& id);

    // find the first job in the queue that is PENDING and assigns it a lease
    // duration and outputs the job via address
    bool lease(long long now, int duration, Job& out_job);

    // if the job is completed, return true. however if it's a zombie then
    // return false
    bool ack(const std::string& id, int epoch);

    // if the job is not completed, return true. however if it's a zombie then
    // return false
    bool nack(const std::string& id, int epoch, int max_attempts);

    // a periodic sweep that finds any jobs that are expired and re-queues them.
    // if max_attemps is reached then it dies instead
    void check_expired(long long now, int max_attempts);
};