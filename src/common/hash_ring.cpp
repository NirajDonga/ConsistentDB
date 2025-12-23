#include "../../include/hash_ring.hpp"
#include <functional>
#include <iostream>
#include <string>
#include <iterator> // Required for std::prev

ConsistentHashRing::ConsistentHashRing(int v_nodes) : virtual_nodes(v_nodes) {}

size_t ConsistentHashRing::hash_key(const std::string& key) {
    return std::hash<std::string>{}(key);
}

void ConsistentHashRing::addNode(const std::string& node_address) {
    for (int i = 0; i < virtual_nodes; ++i) {
        std::string v_node_key = node_address + "#" + std::to_string(i);
        size_t hash = hash_key(v_node_key);
        ring[hash] = node_address;
    }
}

void ConsistentHashRing::removeNode(const std::string& node_address) {
    for (int i = 0; i < virtual_nodes; ++i) {
        std::string v_node_key = node_address + "#" + std::to_string(i);
        size_t hash = hash_key(v_node_key);
        ring.erase(hash);
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

    // Iterate through the ring to find every virtual node belonging to 'new_node'
    for (auto it = ring.begin(); it != ring.end(); ++it) {
        if (it->second == new_node) {
            // Found a virtual node for the new server.
            // The range it owns is (Previous_Hash, Current_Hash].

            size_t end_hash = it->first;
            size_t start_hash;

            // FIX: Handle wrap-around explicitly without mixing iterator types
            if (it == ring.begin()) {
                start_hash = ring.rbegin()->first; // Wrap around to the last element
            } else {
                start_hash = std::prev(it)->first; // Previous element
            }

            // The "Victim" (who held this before) is the NEXT node in the ring.
            auto next_it = std::next(it);
            if (next_it == ring.end()) next_it = ring.begin();
            std::string victim = next_it->second;

            // If the victim is myself, no migration needed (happens if I'm the only node)
            if (victim != new_node) {
                tasks.push_back({victim, start_hash, end_hash});
            }
        }
    }
    return tasks;
}