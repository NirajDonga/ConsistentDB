#include "../../include/hash_ring.hpp"
#include <functional>
#include <iostream>
#include <string>
#include <iterator>

ConsistentHashRing::ConsistentHashRing(int v_nodes) : virtual_nodes(v_nodes) {}

size_t ConsistentHashRing::hash_key(const std::string& key) {
    const size_t FNV_prime = 1099511628211u;
    const size_t offset_basis = 14695981039346656037u;
    size_t hash = offset_basis;
    for (char c : key) {
        hash ^= static_cast<size_t>(c);
        hash *= FNV_prime;
    }
    return hash;
}

void ConsistentHashRing::addNode(const std::string& node_address) {
    for (int i = 0; i < virtual_nodes; ++i) {
        std::string v_node_key = node_address + "#" + std::to_string(i);
        size_t hash = hash_key(v_node_key);
        ring[hash] = node_address;
    }
}

void ConsistentHashRing::removeNode(const std::string& node_address) {
    for (auto it = ring.begin(); it != ring.end(); ) {
        if (it->second == node_address) {
            it = ring.erase(it);
        } else {
            ++it;
        }
    }
}

std::string ConsistentHashRing::getNode(const std::string& key) {
    if (ring.empty()) return "";
    size_t hash = hash_key(key);
    auto it = ring.lower_bound(hash);
    if (it == ring.end()) it = ring.begin();
    return it->second;
}

// [Replace the getRebalancingTasks function in src/common/hash_ring.cpp]
std::vector<MigrationTask> ConsistentHashRing::getRebalancingTasks(const std::string& new_node) {
    std::vector<MigrationTask> tasks;
    if (ring.empty()) return tasks;

    for (auto it = ring.begin(); it != ring.end(); ++it) {
        // We only care about ranges effectively owned by the NEW node
        if (it->second == new_node) {
            size_t end_hash = it->first;
            size_t start_hash;

            // Handle wrap-around
            if (it == ring.begin()) {
                start_hash = ring.rbegin()->first;
            } else {
                start_hash = std::prev(it)->first;
            }

            // SAFETY CHECK: Skip empty ranges to prevent "Move All" bug
            if (start_hash == end_hash) continue;

            // Find the true victim (the first node following us that ISN'T us)
            auto successor_it = std::next(it);
            while (true) {
                if (successor_it == ring.end()) successor_it = ring.begin();
                if (successor_it->second != new_node) break;
                // Stop infinite loop if ring only has the new node
                if (successor_it == it) break;
                successor_it++;
            }
            std::string real_victim = successor_it->second;

            if (real_victim != new_node) {
                tasks.push_back({real_victim, start_hash, end_hash});
            }
        }
    }
    return tasks;
}