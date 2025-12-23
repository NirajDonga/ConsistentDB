#include "../../include/hash_ring.hpp"
#include "../../include/httplib.h"
#include <iostream>
#include <sstream>
#include <vector>
#include <map>

// --- HELPER FUNCTIONS ---
std::string sanitize_host(std::string address) {
    size_t colonPos = address.find(":");
    if (colonPos == std::string::npos) return address;
    std::string ip = address.substr(0, colonPos);
    std::string port = address.substr(colonPos + 1);
    if (ip == "localhost") return "127.0.0.1:" + port;
    return address;
}

bool get_ip_port(const std::string& address, std::string& ip, int& port) {
    std::string clean = sanitize_host(address);
    size_t colon = clean.find(":");
    if (colon == std::string::npos) return false;
    ip = clean.substr(0, colon);
    try { port = std::stoi(clean.substr(colon + 1)); } catch (...) { return false; }
    return true;
}

bool send_put(const std::string& host, const std::string& key, const std::string& val) {
    std::string ip; int port;
    if (!get_ip_port(host, ip, port)) return false;
    httplib::Client cli(ip, port);
    cli.set_connection_timeout(1);
    httplib::Params params; params.emplace("key", key); params.emplace("val", val);
    auto res = cli.Post("/put", params);
    return res && res->status == 200;
}

// --- OPTIMIZED ADD MIGRATION ---
void optimized_rebalance_add(ConsistentHashRing& ring, const std::string& new_node) {
    std::cout << "\033[1;34m[OPTIMIZED] Calculating ranges for " << new_node << "...\033[0m\n";
    auto tasks = ring.getRebalancingTasks(new_node);
    int total_moved = 0;

    std::string new_ip; int new_port;
    if (!get_ip_port(new_node, new_ip, new_port)) return;

    // Persistent client for Destination (New Node)
    httplib::Client dest_cli(new_ip, new_port);
    dest_cli.set_connection_timeout(1);

    for (const auto& task : tasks) {
        std::string src_ip; int src_port;
        if (!get_ip_port(task.source_node, src_ip, src_port)) continue;

        // Persistent client for Source (Old Node)
        httplib::Client src_cli(src_ip, src_port);
        src_cli.set_connection_timeout(1);

        std::string path = "/range?start=" + std::to_string(task.start_hash) +
                           "&end=" + std::to_string(task.end_hash);
        auto res = src_cli.Get(path.c_str());

        if (res && res->status == 200) {
            std::stringstream ss(res->body);
            std::string key, val;
            while (std::getline(ss, key)) {
                if (std::getline(ss, val)) {
                    // 1. Write to NEW (Reuse Dest Client)
                    httplib::Params p_put; p_put.emplace("key", key); p_put.emplace("val", val);
                    if (dest_cli.Post("/put", p_put)) {
                        // 2. Delete from OLD (Reuse Source Client)
                        httplib::Params p_del; p_del.emplace("key", key);
                        src_cli.Post("/del", p_del);
                        total_moved++;
                        std::cout << " -> Pulled '" << key << "' from " << task.source_node << "\n";
                    }
                }
            }
        }
    }
    std::cout << "\033[1;32m[SUCCESS] Rebalanced " << total_moved << " keys.\033[0m\n";
}

// --- OPTIMIZED REMOVE MIGRATION ---
void rebalance_remove(ConsistentHashRing& ring, const std::string& node_to_remove) {
    std::cout << "\033[1;34m[EVACUATION] Draining " << node_to_remove << "...\033[0m\n";
    std::string ip; int port;
    if (!get_ip_port(node_to_remove, ip, port)) return;

    httplib::Client victim_cli(ip, port);
    victim_cli.set_connection_timeout(1);
    auto res = victim_cli.Get("/all");

    if (!res || res->status != 200) {
        std::cout << "[ERROR] Node unreachable. Removing from ring anyway.\n";
        ring.removeNode(node_to_remove);
        return;
    }

    std::map<std::string, std::string> data;
    std::stringstream ss(res->body);
    std::string key, val;
    while(getline(ss, key)) if(getline(ss, val)) data[key] = val;

    ring.removeNode(node_to_remove);
    int count = 0;

    std::map<std::string, httplib::Client*> dest_clients;

    for(auto const& [k, v] : data) {
        std::string target = ring.getNode(k);
        if(!target.empty()) {
            if (dest_clients.find(target) == dest_clients.end()) {
                 std::string t_ip; int t_port;
                 if (get_ip_port(target, t_ip, t_port)) {
                    auto* cli = new httplib::Client(t_ip, t_port);
                    cli->set_connection_timeout(1);
                    dest_clients[target] = cli;
                 }
            }
            if (dest_clients.count(target)) {
                httplib::Params p_put; p_put.emplace("key", k); p_put.emplace("val", v);
                auto put_res = dest_clients[target]->Post("/put", p_put);
                if (put_res && put_res->status == 200) {
                    httplib::Params p_del; p_del.emplace("key", k);
                    victim_cli.Post("/del", p_del);
                    count++;
                }
            }
        }
    }
    for (auto& pair : dest_clients) delete pair.second;
    std::cout << "\033[1;32m[SUCCESS] Evacuated " << count << " keys.\033[0m\n";
}

int main() {
    ConsistentHashRing ring;
    std::cout.setf(std::ios::unitbuf);

    std::cout << "--- Optimized KV Client ---\n";
    std::cout << "Commands: ADD <host:port>, REMOVE <host:port>, SET <k> <v>, GET <k>, EXIT\n";

    std::string line, cmd, arg1, arg2;
    while (true) {
        std::cout << "> ";
        if (!std::getline(std::cin, line)) break;
        if (line.empty()) continue;
        std::stringstream ss(line); ss >> cmd;

        if (cmd == "EXIT") break;
        else if (cmd == "ADD") {
            ss >> arg1;
            if (arg1.empty() || arg1.find(":") == std::string::npos) {
                std::cout << "[ERROR] Usage: ADD 127.0.0.1:8080\n";
            } else {
                std::string fixed = sanitize_host(arg1);
                ring.addNode(fixed);
                std::cout << "Added " << fixed << "\n";
                optimized_rebalance_add(ring, fixed);
            }
        }
        else if (cmd == "REMOVE") {
            ss >> arg1;
            if (arg1.empty() || arg1.find(":") == std::string::npos) {
                std::cout << "[ERROR] Usage: REMOVE 127.0.0.1:8080\n";
            } else {
                rebalance_remove(ring, sanitize_host(arg1));
            }
        }
        else if (cmd == "SET") {
            ss >> arg1; std::getline(ss, arg2);
            if (!arg2.empty()) arg2 = arg2.substr(1);
            if (arg1.empty() || arg2.empty()) {
                std::cout << "Usage: SET <key> <value>\n";
            } else {
                std::string target = ring.getNode(arg1);
                if (!target.empty()) send_put(target, arg1, arg2);
                else std::cout << "No servers available.\n";
            }
        }
        else if (cmd == "GET") {
            ss >> arg1;
            if (!arg1.empty()) {
                std::string target = ring.getNode(arg1);
                if (!target.empty()) {
                    std::string ip; int port;
                    if (get_ip_port(target, ip, port)) {
                        httplib::Client cli(ip, port);
                        cli.set_connection_timeout(1);
                        auto res = cli.Get(("/get?key=" + arg1).c_str());
                        if (res && res->status == 200) std::cout << "Found on " << target << ": " << res->body << "\n";
                        else std::cout << "Not found\n";
                    }
                } else { std::cout << "No servers available.\n"; }
            }
        }
    }
    return 0;
}