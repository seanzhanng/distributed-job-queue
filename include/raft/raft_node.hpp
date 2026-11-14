#pragma once

#include "raft/raft_types.hpp"
#include <mutex>
#include <vector>

namespace raft {

class RaftNode {
  private:
    int id;
    std::vector<int> peer_ids; // ids of all other nodes in cluster
    std::mutex mtx;

    int current_term; // latest term node has seen
    int voted_for;    // candidate id that received vote in current term
    std::vector<LogEntry> log;

    RaftState state; // current role: FOLLOWER, CANDIDATE, or LEADER

    // role transitions and outbound rpc triggers
    void become_follower(int new_term);
    void start_election();
    void send_heartbeats();

  public:
    // constructor
    RaftNode(int id, const std::vector<int>& peer_ids);

    // incoming rpc handlers
    RequestVoteResponse handle_request_vote(const RequestVoteRequest& req);
    AppendEntriesResponse
    handle_append_entries(const AppendEntriesRequest& req);
};

} // namespace raft