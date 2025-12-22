#pragma once
#include <string>
#include <map>
#include <vector>

class ConsistentHashRing {
private:
    std::map<size_t, std::string> ring;
    int virtual_nodes;
    size_t hash_key(const std::string& key);

public:
    ConsistentHashRing(int v_nodes = 100);
    void addNode(const std::string& node_address);
    void removeNode(const std::string& node_address);

    // --- THIS LINE IS MISSING OR NOT SAVED ---
    std::string getNode(const std::string& key);
};