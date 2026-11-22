#include "raft/raft_node.hpp"
#include "raft/raft_types.hpp"
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

namespace raft {

RaftNode::RaftNode(
    int id, const std::vector<int>& peer_ids,
    std::function<RequestVoteResponse(int, const RequestVoteRequest&)> const&
        send_request_vote,
    std::function<AppendEntriesResponse(
        int, const AppendEntriesRequest&)> const& send_append_entries)
    : id(id), peer_ids(peer_ids), current_term(0), voted_for(-1),
      commit_index(0), state(RaftState::FOLLOWER), running(false),
      send_request_vote(send_request_vote),
      send_append_entries(send_append_entries) {
    reset_election_timer();
}

void RaftNode::become_follower(int new_term) {
    state = RaftState::FOLLOWER;
    current_term = new_term;
    voted_for = -1;
    reset_election_timer();
}

void RaftNode::start_election() {
    // set state to candidate, increment current term and vote for self
    state = RaftState::CANDIDATE;
    current_term++;
    voted_for = id;
    reset_election_timer();

    // snapshot the attributes relevant to RequestVoteRequest as these can
    // change at any time
    int term = current_term;
    int candidate_id = id;
    int last_log_index = static_cast<int>(log.size());
    int last_log_term = log.empty() ? 0 : log.back().term;

    // create a pointer to a counter so votes from other nodes can be counted.
    // initialize with 1 (candidate's own vote)
    std::shared_ptr<std::atomic<int>> votes_received =
        std::make_shared<std::atomic<int>>(1);

    // calculate majority (min votes needed to become leader)
    int majority = (static_cast<int>(peer_ids.size()) + 1) / 2 + 1;

    for (int peer_id : peer_ids) {
        // spawn a new thread for each peer node and send a vote request
        std::thread([this, peer_id, term, candidate_id, last_log_index,
                     last_log_term, votes_received, majority]() {
            RequestVoteRequest req;
            req.term = term;
            req.candidate_id = candidate_id;
            req.last_log_index = last_log_index;
            req.last_log_term = last_log_term;

            RequestVoteResponse resp = send_request_vote(peer_id, req);

            std::lock_guard<std::mutex> lock(mtx);
            if (resp.term > current_term) {
                become_follower(resp.term);
                return;
            }

            // ignore replies from an election that already ended or been
            // replaced by a newer one
            if (state != RaftState::CANDIDATE || current_term != term) {
                return;
            }
            if (resp.vote_granted) {
                // once a vote has been granted, increment votes received
                (*votes_received)++;

                if (state == RaftState::CANDIDATE &&
                    *votes_received >= majority) {
                    // once majority votes has been reached, this node becomes
                    // leader and immediately sends heartbeats so other nodes
                    // know this one is leader
                    state = RaftState::LEADER;
                    send_heartbeats();

                    // spawn a thread to keep sending heartbeats every 50ms
                    std::thread([this]() {
                        while (running) {
                            std::this_thread::sleep_for(
                                std::chrono::milliseconds(50));

                            std::lock_guard<std::mutex> lock(mtx);
                            if (state != RaftState::LEADER) {
                                break;
                            }

                            send_heartbeats();
                        }
                    }).detach();
                }
            }
        }).detach();
    }
}

void RaftNode::send_heartbeats() {
    // only leaders can send heartbeats
    if (state != RaftState::LEADER) {
        return;
    }

    // snapshot the attributes relevant to AppendEntriesRequest as these can
    // change at any time
    int term = current_term;
    int leader_id = id;
    int prev_log_index = static_cast<int>(log.size());
    int prev_log_term = log.empty() ? 0 : log.back().term;
    int leader_commit = commit_index;

    for (int peer_id : peer_ids) {
        // spawn a new thread for each peer node and send a heartbeat entry
        std::thread([this, peer_id, term, leader_id, prev_log_index,
                     prev_log_term, leader_commit]() {
            AppendEntriesRequest req;
            req.term = term;
            req.leader_id = leader_id;
            req.prev_log_index = prev_log_index;
            req.prev_log_term = prev_log_term;
            req.entries = {}; // heartbeats have no entries
            req.leader_commit = leader_commit;

            AppendEntriesResponse resp = send_append_entries(peer_id, req);

            std::lock_guard<std::mutex> lock(mtx);
            if (resp.term > current_term) {
                become_follower(resp.term);
            }
        }).detach();
    }
}

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

        // start an election if this non-leader's election timeout has been
        // exceeded
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

RaftState RaftNode::get_state() {
    std::lock_guard<std::mutex> lock(mtx);
    return state;
}

int RaftNode::get_term() {
    std::lock_guard<std::mutex> lock(mtx);
    return current_term;
}

int RaftNode::get_id() {
    return id;
}

} // namespace raft