#include "raft/raft_node.hpp"
#include "raft/raft_types.hpp"
#include <functional>
#include <mutex>
#include <random>
#include <vector>

namespace raft {

RaftNode::RaftNode(
    int id, const std::vector<int>& peer_ids,
    std::function<RequestVoteResponse(int, const RequestVoteRequest&)> const&
        send_request_vote,
    std::function<AppendEntriesResponse(
        int, const AppendEntriesRequest&)> const& send_append_entries)
    : id(id), peer_ids(peer_ids), current_term(0), voted_for(-1),
      state(RaftState::FOLLOWER), send_request_vote(send_request_vote),
      send_append_entries(send_append_entries), running(false) {
    reset_election_timer();
}

void RaftNode::become_follower(int new_term) {
    state = RaftState::FOLLOWER;
    current_term = new_term;
    voted_for = -1;
    reset_election_timer();
}

void RaftNode::start_election() {}

void RaftNode::send_heartbeats() {}

void RaftNode::reset_election_timer() {
    thread_local std::mt19937 generator{std::random_device{}()};
    std::uniform_int_distribution<int> election_timeout_ms(150, 300);

    election_reset_event = std::chrono::steady_clock::now();
    election_timeout =
        std::chrono::milliseconds(election_timeout_ms(generator));
}

void RaftNode::run_election_timer() {
    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::lock_guard<std::mutex> lock(mtx);
        if (state != RaftState::LEADER &&
            (std::chrono::steady_clock::now() - election_reset_event) >=
                election_timeout) {
            start_election();
        }
    }
}

void RaftNode::start() {
    std::lock_guard<std::mutex> lock(mtx);

    reset_election_timer();
    running = true;

    timer_thread = std::thread(&RaftNode::run_election_timer, this);
}

void RaftNode::stop() {
    running = false;

    if (timer_thread.joinable()) {
        timer_thread.join();
    }
}

RequestVoteResponse
RaftNode::handle_request_vote(const RequestVoteRequest& req) {
    std::lock_guard<std::mutex> lock(mtx);

    // if the candidate's term is greater than this node's current term, convert
    // to follower
    if (req.term > current_term) {
        become_follower(req.term);
    }

    // instantiate the response RequestVoteResponse
    RequestVoteResponse resp;
    resp.term = current_term;
    resp.vote_granted = false;

    // if the candidate's term is too small, it's definitely behind so
    // auto-reject
    if (req.term < current_term) {
        return resp;
    }

    // compute the current node's last log index and last log term to compare
    // with the candidate's log freshness
    int last_log_index = static_cast<int>(log.size());
    int last_log_term = log.empty() ? 0 : log.back().term;

    // check that the candidates last logged term is higher (means it's more up
    // to date). if their terms are the same then make sure its last log index
    // is as up to date as the current node's last log index
    if ((req.last_log_term > last_log_term) ||
        (req.last_log_term == last_log_term &&
         req.last_log_index >= last_log_index)) {

        // ensure the current node hasn't voted yet or in the rare edge case,
        // the candidate had a network disruptance and asked for a vote again
        if (voted_for == -1 || voted_for == req.candidate_id) {
            // set current node's vote to the candidate and the
            // RequestVoteResponse's vote_granted to true
            voted_for = req.candidate_id;
            resp.vote_granted = true;
            reset_election_timer();
        }
    }

    return resp;
}

AppendEntriesResponse
RaftNode::handle_append_entries(const AppendEntriesRequest& req) {
    std::lock_guard<std::mutex> lock(mtx);

    // if the leader's term is greater than this node's current term, convert
    // to follower
    if (req.term > current_term) {
        become_follower(req.term);
    }

    // instantiate the response AppendEntriesResponse
    AppendEntriesResponse resp;
    resp.term = current_term;
    resp.success = false;

    // if the leader's term is too small, it's definitely behind so
    // auto-reject
    if (req.term < current_term) {
        return resp;
    }

    // if this node is a candidate, step down to follower to respect the
    // leader's authority in the current term
    if (state == RaftState::CANDIDATE) {
        state = RaftState::FOLLOWER;
    }

    reset_election_timer();

    // accept the append request and mark it successful
    resp.success = true;

    return resp;
}

} // namespace raft