#include "../../include/hash_ring.hpp"
#include <functional>
#include <iostream>
#include <string>
#include <iterator>

ConsistentHashRing::ConsistentHashRing(int v_nodes) : virtual_nodes(v_nodes) {}

size_t ConsistentHashRing::hash_key(const std::string& key) {
    // 1. FNV-1a 64-bit Base
    const size_t FNV_prime = 1099511628211u;
    const size_t offset_basis = 14695981039346656037u;

    size_t hash = offset_basis;
    for (char c : key) {
        hash ^= static_cast<size_t>(c);
        hash *= FNV_prime;
    }

    // 2. MURMUR3 AVALANCHE MIXER (The Fix)
    hash ^= hash >> 33;
    hash *= 0xff51afd7ed558ccd;
    hash ^= hash >> 33;
    hash *= 0xc4ceb9fe1a85ec53;
    hash ^= hash >> 33;

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
    std::cout << "[Ring] Request received to remove node: " << node_address << "...\n";

    int removed_count = 0;
    for (auto it = ring.begin(); it != ring.end(); ) {
        if (it->second == node_address) {
            it = ring.erase(it);
            removed_count++;
        } else {
            ++it;
        }
    }

    if (removed_count > 0) {
        std::cout << "[Ring] Success: Removed " << removed_count << " virtual nodes for " << node_address << ".\n";
    } else {
        std::cout << "[Ring] Warning: Node " << node_address << " was not found in the ring.\n";
    }
}

std::string ConsistentHashRing::getNode(const std::string& key) {
    if (ring.empty()) return "";
    size_t hash = hash_key(key);
    auto it = ring.lower_bound(hash);
    if (it == ring.end()) it = ring.begin();
    return it->second;
}

std::vector<MigrationTask> ConsistentHashRing::getRebalancingTasks(const std::string& new_node) {
    std::vector<MigrationTask> tasks;
    if (ring.empty()) return tasks;

    std::cout << "[Ring] Calculating rebalancing tasks for " << new_node << "...\n";

    for (int i = 0; i < virtual_nodes; ++i) {
        // 1. Reconstruct the virtual key and hash
        std::string v_node_key = new_node + "#" + std::to_string(i);
        size_t hash = hash_key(v_node_key);

        // 2. Direct Lookup: Jump straight to this node in the map
        auto it = ring.find(hash);

        // Safety check: ensure the node is actually in the ring
        if (it == ring.end()) continue;

        size_t end_hash = it->first;
        size_t start_hash;

        // 3. Determine the Range Start (Previous Node)
        if (it == ring.begin()) {
            start_hash = ring.rbegin()->first; // Wrap around to the end
        } else {
            start_hash = std::prev(it)->first;
        }

        // SAFETY: Skip zero-length ranges
        if (start_hash == end_hash) continue;

        // 4. Find Victim (Who owned this before?)
        // Look clockwise (forward) for the first node that ISN'T us.
        auto successor_it = std::next(it);
        while (true) {
            if (successor_it == ring.end()) successor_it = ring.begin();
            if (successor_it->second != new_node) break; // Found a different server
            if (successor_it == it) break; // We looped entirely around (only 1 node exists)
            successor_it++;
        }

        std::string real_victim = successor_it->second;

        if (real_victim != new_node) {
            tasks.push_back({real_victim, start_hash, end_hash});
        }
    }
    return tasks;
}