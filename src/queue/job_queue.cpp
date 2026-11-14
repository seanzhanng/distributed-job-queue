#include "queue/job_queue.hpp"
#include "queue/job.hpp"

/*
 * JOB QUEUE STATE & CONCURRENCY INVARIANTS
 * ----------------------------------------
 * All methods acquire `mtx` to serialize state transitions and prevent data
 * races.
 *
 * 1. Normal Lifecycle:
 *    enqueue() -> PENDING (epoch = 0, attempts = 0)
 *    lease()   -> LEASED  (epoch++, attempts++, expires_at = now + duration)
 *    ack()     -> COMPLETED (if state == LEASED && epoch matches)
 *    nack()    -> PENDING or DEAD (if state == LEASED && epoch matches, based
 * on max_attempts)
 *
 * 2. Zombie / Stale Worker Fencing:
 *    - If a worker stalls past `lease_expires_at`, `check_expired()` sweeps the
 * job:
 *        * attempts < max_attempts  -> resets to PENDING (ready for re-lease
 * with epoch + 1)
 *        * attempts >= max_attempts -> transitions to DEAD (terminal failure)
 *    - When the stalled (zombie) worker eventually wakes up and calls ack() or
 * nack(), either `state != LEASED` (still PENDING/DEAD) or `epoch` no longer
 * matches (re-leased). Both cases return `false`, fencing out the stale worker.
 *
 * 3. Benign Expiration Race (Expired in clock time, before check_expired()
 * ticks):
 *    - `lease()` ONLY picks PENDING jobs, so a job in LEASED limbo is never
 * handed to two workers.
 *    - If a slow worker calls ack()/nack() BEFORE `check_expired()` runs, it
 * still holds the valid epoch and succeeds (`true`) with zero duplicate work.
 *    - If `check_expired()` grabs `mtx` first, it revokes the lease (PENDING or
 * DEAD) and the late worker's ack()/nack() gets rejected (`false`).
 */

namespace queue {

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

} // namespace queue