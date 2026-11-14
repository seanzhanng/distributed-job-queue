#pragma once

#include <string>
#include <vector>

namespace raft {

enum class RaftState { FOLLOWER, CANDIDATE, LEADER };

struct LogEntry {
    int term;
    std::string command;
};

struct RequestVoteRequest {
    int term;           // candidate's term
    int candidate_id;   // candidate requesting vote
    int last_log_index; // index if candidate's last log entry
    int last_log_term;  // term of candidate's last log entry
};

struct RequestVoteResponse {
    int term;          // current term, for candidate to update itself
    bool vote_granted; // true means candidate received vote
};

struct AppendEntriesRequest {
    int term;           // leader's current term
    int leader_id;      // so follower can redirect clients
    int prev_log_index; // index of log entry immediatley preceding new one
    int prev_log_term;  // term of prev_log_index entry
    std::vector<LogEntry> entries; // log entries to store (empty for heartbeat)
    int leader_commit;             // leader's commit index
};

struct AppendEntriesResponse {
    int term;     // current term, for leader to update itself
    bool success; // true if follower contained entry matching prev_log_index
                  // and prev_log_term
};

} // namespace raft