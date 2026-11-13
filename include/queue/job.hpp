#pragma once

#include <string>

// the state a job is currently in
enum class JobState {
    PENDING,   // refers to a job still in the queue
    LEASED,    // a job that is currently inflight
    COMPLETED, // a job that has been completed
    DEAD       // a job that was unable to be completed and reached max attempts
};

struct Job {
    std::string id;
    JobState state;
    int lease_epoch;            // to prevent zombie nodes from working the job
    long long lease_expires_at; // time the job expires at
    int attempts;               // how many times this job has been tried
};