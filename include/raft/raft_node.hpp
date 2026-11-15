#pragma once

#include "raft/raft_types.hpp"
#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <thread>
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

    std::chrono::steady_clock::time_point election_reset_event;
    std::chrono::milliseconds election_timeout;
    std::atomic<bool> running;
    std::thread timer_thread;

    // outbound rpc callbacks
    std::function<RequestVoteResponse(int, const RequestVoteRequest&)>
        send_request_vote;
    std::function<AppendEntriesResponse(int, const AppendEntriesRequest&)>
        send_append_entries;

    // role transitions and outbound rpc triggers
    void become_follower(int new_term);
    void start_election();
    void send_heartbeats();

    void reset_election_timer();
    void run_election_timer();

  public:
    // constructor
    RaftNode(int id, const std::vector<int>& peer_ids,
             std::function<RequestVoteResponse(
                 int, const RequestVoteRequest&)> const& send_request_vote,
             std::function<AppendEntriesResponse(
                 int, const AppendEntriesRequest&)> const& send_append_entries);

    void start();
    void stop();

    // incoming rpc handlers
    RequestVoteResponse handle_request_vote(const RequestVoteRequest& req);
    AppendEntriesResponse
    handle_append_entries(const AppendEntriesRequest& req);
};

} // namespace raft