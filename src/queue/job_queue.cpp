#include "queue/job_queue.hpp"
#include "queue/job.hpp"

void JobQueue::enqueue(const std::string& id) {
    std::lock_guard<std::mutex> lock(mtx);

    Job job;
    job.id = id;
    job.state = JobState::PENDING;
    job.lease_epoch = 0;
    job.lease_expires_at = 0;
    job.attempts = 0;

    job_queue.push_back(job);
}

bool JobQueue::lease(long long now, int duration, Job& out_job) {
    std::lock_guard<std::mutex> lock(mtx);

    for (Job& job : job_queue) {
        if (job.state == JobState::PENDING) {
            job.state = JobState::LEASED;
            job.lease_epoch++;
            job.lease_expires_at = now + duration;
            job.attempts++;
            out_job = job;
            return true;
        }
    }
    return false;
}

bool JobQueue::ack(const std::string& id, int epoch) {
    std::lock_guard<std::mutex> lock(mtx);

    for (Job& job : job_queue) {
        // as long as the caller is not a zombie, this will always return true
        if (job.id == id && job.state == JobState::LEASED &&
            job.lease_epoch == epoch) {
            job.state = JobState::COMPLETED;
            return true;
        }
    }
    // zombies always return false
    return false;
}

bool JobQueue::nack(const std::string& id, int epoch, int max_attempts) {
    std::lock_guard<std::mutex> lock(mtx);

    for (Job& job : job_queue) {
        // as long as the caller is not a zombie, this will always return true
        if (job.id == id && job.state == JobState::LEASED &&
            job.lease_epoch == epoch) {
            if (job.attempts >= max_attempts) {
                job.state = JobState::DEAD;
            } else {
                job.state = JobState::PENDING;
            }
            return true;
        }
    }
    // zombies always return false
    return false;
}

void JobQueue::check_expired(long long now, int max_attempts) {
    std::lock_guard<std::mutex> lock(mtx);

    for (Job& job : job_queue) {
        if (job.state == JobState::LEASED && job.lease_expires_at <= now) {
            if (job.attempts >= max_attempts) {
                job.state = JobState::DEAD;
            } else {
                job.state = JobState::PENDING;
            }
        }
    }
}