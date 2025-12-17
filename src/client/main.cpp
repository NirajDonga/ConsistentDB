#include "../../include/hash_ring.hpp"
#include "../../include/httplib.h"
#include <iostream>

void put_data(ConsistentHashRing& ring, const std::string& key, const std::string& val) {
    std::string target = ring.getNode(key); 
    
    // Parse "localhost:8081"
    std::string ip = target.substr(0, target.find(":"));
    int port = std::stoi(target.substr(target.find(":") + 1));

    httplib::Client cli(ip, port);
    httplib::Params params;
    params.emplace("key", key);
    params.emplace("val", val);

    auto res = cli.Post("/put", params);
    
    if (res && res->status == 200) {
        std::cout << "[Client] Key '" << key << "' -> Hashed to Node " << port << " (Success)\n";
    } else {
        std::cout << "[Client] Key '" << key << "' -> Node " << port << " (FAILED - Is it running?)\n";
    }
}

int main() {
    ConsistentHashRing ring;

    // Setup: Add your "N Databases"
    ring.addNode("localhost:8081");
    ring.addNode("localhost:8082");
    ring.addNode("localhost:8083");

    std::cout << "--- Distributed Client Started ---\n";
    
    // Insert dummy data
    put_data(ring, "user_id_1", "Alice");
    put_data(ring, "user_id_2", "Bob");
    put_data(ring, "user_id_3", "Charlie");
    put_data(ring, "product_55", "MacBook");
    put_data(ring, "order_777", "Pizza");

    return 0;
}