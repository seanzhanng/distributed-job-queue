#include "queue/job.hpp"
#include "queue/job_queue.hpp"
#include <cassert>
#include <iostream>

constexpr int DEFAULT_MAX_ATTEMPTS = 3;
constexpr int DEFAULT_LEASE_DURATION = 2000;
constexpr long long START_TIME = 1000;

void test_basic_lifecycle() {
    queue::JobQueue q;
    queue::Job out;

    // Empty queue returns false
    assert(!q.lease(START_TIME, DEFAULT_LEASE_DURATION, out));

    // Enqueue and lease
    q.enqueue("job-1");
    assert(q.lease(START_TIME, DEFAULT_LEASE_DURATION, out));
    assert(out.id == "job-1");
    assert(out.state == queue::JobState::LEASED);
    assert(out.lease_epoch == 1);
    assert(out.attempts == 1);
    assert(out.lease_expires_at == START_TIME + DEFAULT_LEASE_DURATION);

    // No more pending jobs
    assert(!q.lease(START_TIME, DEFAULT_LEASE_DURATION, out));

    // Valid ack completes the job
    assert(q.ack("job-1", 1));

    // Double ack / late nack / re-lease are all rejected
    assert(!q.ack("job-1", 1));
    assert(!q.nack("job-1", 1, DEFAULT_MAX_ATTEMPTS));
    assert(!q.lease(START_TIME + 500, DEFAULT_LEASE_DURATION, out));

    std::cout << "[PASS] test_basic_lifecycle\n";
}

void test_zombie_worker_fencing() {
    queue::JobQueue q;
    q.enqueue("job-1");

    const long long expires_at = START_TIME + DEFAULT_LEASE_DURATION;

    queue::Job worker_a_job;
    assert(q.lease(START_TIME, DEFAULT_LEASE_DURATION, worker_a_job));
    assert(worker_a_job.lease_epoch == 1);

    // 1ms before expiration, sweep does nothing
    q.check_expired(expires_at - 1, DEFAULT_MAX_ATTEMPTS);
    queue::Job tmp;
    assert(!q.lease(expires_at - 1, DEFAULT_LEASE_DURATION, tmp));

    // At expiration, sweep re-queues the job
    q.check_expired(expires_at, DEFAULT_MAX_ATTEMPTS);

    // Worker B leases the re-queued job with incremented epoch
    queue::Job worker_b_job;
    assert(q.lease(expires_at, DEFAULT_LEASE_DURATION, worker_b_job));
    assert(worker_b_job.id == "job-1");
    assert(worker_b_job.lease_epoch == 2);
    assert(worker_b_job.attempts == 2);

    // Stale Worker A wakes up and tries to ack/nack with epoch 1 -> fenced out
    assert(!q.ack(worker_a_job.id, worker_a_job.lease_epoch));
    assert(!q.nack(worker_a_job.id, worker_a_job.lease_epoch, DEFAULT_MAX_ATTEMPTS));

    // Worker B still owns epoch 2 and succeeds
    assert(q.ack(worker_b_job.id, worker_b_job.lease_epoch));

    std::cout << "[PASS] test_zombie_worker_fencing\n";
}

void test_retry_and_dead_letter_via_nack() {
    queue::JobQueue q;
    q.enqueue("job-1");

    queue::Job out;
    long long now = START_TIME;

    for (int attempt = 1; attempt <= DEFAULT_MAX_ATTEMPTS; attempt++) {
        assert(q.lease(now, DEFAULT_LEASE_DURATION, out));
        assert(out.attempts == attempt);
        assert(q.nack("job-1", out.lease_epoch, DEFAULT_MAX_ATTEMPTS));
        now += 100;
    }

    // Dead job can never be leased or acked again
    assert(!q.lease(now, DEFAULT_LEASE_DURATION, out));
    assert(!q.ack("job-1", DEFAULT_MAX_ATTEMPTS));

    std::cout << "[PASS] test_retry_and_dead_letter_via_nack\n";
}

void test_dead_letter_via_expiration() {
    queue::JobQueue q;
    q.enqueue("job-1");

    queue::Job out;
    long long now = START_TIME;

    for (int attempt = 1; attempt <= DEFAULT_MAX_ATTEMPTS; attempt++) {
        assert(q.lease(now, DEFAULT_LEASE_DURATION, out));
        assert(out.attempts == attempt);
        now += DEFAULT_LEASE_DURATION;
        q.check_expired(now, DEFAULT_MAX_ATTEMPTS);
    }

    // Cannot be leased again after hitting DEFAULT_MAX_ATTEMPTS
    assert(!q.lease(now + 100, DEFAULT_LEASE_DURATION, out));

    std::cout << "[PASS] test_dead_letter_via_expiration\n";
}

int main() {
    test_basic_lifecycle();
    test_zombie_worker_fencing();
    test_retry_and_dead_letter_via_nack();
    test_dead_letter_via_expiration();

    std::cout << "\nAll JobQueue tests passed!\n";
    return 0;
}