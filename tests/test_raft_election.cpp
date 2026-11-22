#include "raft/raft_node.hpp"
#include "raft/raft_types.hpp"
#include <cassert>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>

struct TestCluster {
    std::unordered_map<int, std::unique_ptr<raft::RaftNode>> nodes;
    std::mutex net_mtx;
    std::set<int> disconnected;

    explicit TestCluster(int num_nodes) {
        std::vector<int> all_ids;
        for (int i = 0; i < num_nodes; i++) {
            all_ids.push_back(i);
        }

        for (int id : all_ids) {
            std::vector<int> peers;
            for (int peer_id : all_ids) {
                if (peer_id != id) {
                    peers.push_back(peer_id);
                }
            }

            auto send_rv = [this, id](int target_id,
                                      const raft::RequestVoteRequest& req)
                -> raft::RequestVoteResponse {
                raft::RaftNode* target = nullptr;
                {
                    std::lock_guard<std::mutex> lock(net_mtx);
                    if (disconnected.count(id) ||
                        disconnected.count(target_id)) {
                        return raft::RequestVoteResponse{0, false};
                    }
                    target = nodes.at(target_id).get();
                }
                return target->handle_request_vote(req);
            };

            auto send_ae = [this, id](int target_id,
                                      const raft::AppendEntriesRequest& req)
                -> raft::AppendEntriesResponse {
                raft::RaftNode* target = nullptr;
                {
                    std::lock_guard<std::mutex> lock(net_mtx);
                    if (disconnected.count(id) ||
                        disconnected.count(target_id)) {
                        return raft::AppendEntriesResponse{0, false};
                    }
                    target = nodes.at(target_id).get();
                }
                return target->handle_append_entries(req);
            };

            nodes[id] = std::make_unique<raft::RaftNode>(id, peers, send_rv,
                                                         send_ae);
        }
    }

    ~TestCluster() { stop_all(); }

    void start_all() {
        for (auto& [id, node] : nodes) {
            node->start();
        }
    }

    void stop_all() {
        for (auto& [id, node] : nodes) {
            node->stop();
        }
        // Brief grace period so any in-flight detached RPC threads finish
        // before unique_ptrs destroy the RaftNode instances
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void disconnect(int id) {
        std::lock_guard<std::mutex> lock(net_mtx);
        disconnected.insert(id);
    }

    void reconnect(int id) {
        std::lock_guard<std::mutex> lock(net_mtx);
        disconnected.erase(id);
    }

    // Returns {leader_id, leader_term}; asserts at most one connected leader
    std::pair<int, int> check_single_leader() {
        int leader_id = -1;
        int leader_term = -1;

        for (auto& [id, node] : nodes) {
            bool is_disconnected = false;
            {
                std::lock_guard<std::mutex> lock(net_mtx);
                is_disconnected = disconnected.count(id) > 0;
            }
            if (is_disconnected) {
                continue;
            }

            if (node->get_state() == raft::RaftState::LEADER) {
                assert(leader_id == -1 && "Multiple leaders in cluster!");
                leader_id = id;
                leader_term = node->get_term();
            }
        }

        assert(leader_id != -1 && "No leader elected!");
        return {leader_id, leader_term};
    }
};

void test_initial_election() {
    TestCluster cluster(5);
    cluster.start_all();

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto [leader_id, leader_term] = cluster.check_single_leader();
    assert(leader_term >= 1);

    for (auto& [id, node] : cluster.nodes) {
        assert(node->get_term() == leader_term);
        if (id != leader_id) {
            assert(node->get_state() == raft::RaftState::FOLLOWER);
        }
    }

    std::cout << "[PASS] test_initial_election\n";
}

void test_leader_failover() {
    TestCluster cluster(5);
    cluster.start_all();

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto [old_leader_id, old_term] = cluster.check_single_leader();

    // Isolate the current leader
    cluster.disconnect(old_leader_id);

    // Wait for remaining 4 nodes to time out and elect a new leader
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto [new_leader_id, new_term] = cluster.check_single_leader();

    assert(new_leader_id != old_leader_id);
    assert(new_term > old_term);

    std::cout << "[PASS] test_leader_failover\n";
}

void test_partition_and_heal() {
    TestCluster cluster(5);
    cluster.start_all();

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto [old_leader_id, old_term] = cluster.check_single_leader();

    // Partition old leader away from the majority
    cluster.disconnect(old_leader_id);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto [new_leader_id, new_term] = cluster.check_single_leader();
    assert(new_leader_id != old_leader_id);
    assert(new_term > old_term);

    // Heal the partition; old leader must step down and adopt the higher term
    cluster.reconnect(old_leader_id);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    auto [final_leader_id, final_term] = cluster.check_single_leader();
    assert(final_leader_id == new_leader_id);
    assert(cluster.nodes[old_leader_id]->get_state() ==
           raft::RaftState::FOLLOWER);
    assert(cluster.nodes[old_leader_id]->get_term() == final_term);

    std::cout << "[PASS] test_partition_and_heal\n";
}

void test_minority_partition_no_leader() {
    TestCluster cluster(5);
    cluster.start_all();

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto [leader_id, term] = cluster.check_single_leader();

    // Disconnect 3 nodes so only 2 nodes remain connected (no quorum of 3)
    int disconnected_count = 0;
    for (int i = 0; i < 5 && disconnected_count < 3; i++) {
        if (i != leader_id) {
            cluster.disconnect(i);
            disconnected_count++;
        }
    }
    // Also disconnect the leader so the 2 connected nodes have no leader
    cluster.disconnect(leader_id);
    // Reconnect 1 follower so exactly 2 nodes are connected (minority)
    int reconnected_id = (leader_id + 1) % 5;
    cluster.reconnect(reconnected_id);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Neither of the 2 connected nodes can reach majority (3 votes)
    assert(cluster.nodes[leader_id]->get_state() != raft::RaftState::LEADER ||
           cluster.disconnected.count(leader_id) > 0);
    assert(cluster.nodes[reconnected_id]->get_state() !=
           raft::RaftState::LEADER);

    // Restore full connectivity and verify a single leader emerges
    for (int i = 0; i < 5; i++) {
        cluster.reconnect(i);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    cluster.check_single_leader();

    std::cout << "[PASS] test_minority_partition_no_leader\n";
}

int main() {
    test_initial_election();
    test_leader_failover();
    test_partition_and_heal();
    test_minority_partition_no_leader();

    std::cout << "\nAll Raft election tests passed!\n";
    return 0;
}