#include "../../include/hash_ring.hpp"
#include <functional>
#include <iostream>
#include <string>

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