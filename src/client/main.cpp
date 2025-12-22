#include "../../include/hash_ring.hpp"
#include "../../include/httplib.h"
#include <iostream>
#include <sstream>
#include <vector>

// --- HELPER: Force IPv4 for Windows ---
// If the user types "localhost", we switch it to "127.0.0.1" to avoid connection errors.
std::string sanitize_host(std::string address) {
    std::string ip = address.substr(0, address.find(":"));
    std::string port = address.substr(address.find(":") + 1);

    if (ip == "localhost") {
        return "127.0.0.1:" + port;
    }
    return address;
}

// Helper to print colored text
void print_success(const std::string& msg) { std::cout << "\033[1;32m" << msg << "\033[0m\n"; }
void print_error(const std::string& msg)   { std::cout << "\033[1;31m" << msg << "\033[0m\n"; }
void print_info(const std::string& msg)    { std::cout << "\033[1;34m" << msg << "\033[0m\n"; }

// --- SEND DATA (WRITE) ---
void put_data(ConsistentHashRing& ring, const std::string& key, const std::string& val) {
    std::string target = ring.getNode(key);

    if (target.empty()) {
        print_error("[FAIL] No servers available in the ring! Use ADD <host>:<port> first.");
        return;
    }

    // Sanitize ensures we always use 127.0.0.1
    target = sanitize_host(target);

    size_t colonPos = target.find(":");
    std::string ip = target.substr(0, colonPos);
    int port = std::stoi(target.substr(colonPos + 1));

    httplib::Client cli(ip, port);
    cli.set_connection_timeout(2); // 2 Second Timeout

    // Explicitly create Params
    httplib::Params params;
    params.emplace("key", key);
    params.emplace("val", val);

    auto res = cli.Post("/put", params);

    if (res && res->status == 200) {
        print_success("[OK] Stored '" + key + "' on Node " + target);
    } else {
        std::string err = res ? std::to_string(res->status) : "Connection Refused";
        print_error("[FAIL] Could not write to Node " + target + " (Error: " + err + ")");
    }
}

// --- GET DATA (READ) ---
void get_data(ConsistentHashRing& ring, const std::string& key) {
    std::string target = ring.getNode(key);

    if (target.empty()) {
        print_error("[FAIL] No servers available in the ring! Use ADD <host>:<port> first.");
        return;
    }

    target = sanitize_host(target);

    size_t colonPos = target.find(":");
    std::string ip = target.substr(0, colonPos);
    int port = std::stoi(target.substr(colonPos + 1));

    httplib::Client cli(ip, port);
    cli.set_connection_timeout(2);

    auto res = cli.Get(("/get?key=" + key).c_str());

    if (res && res->status == 200) {
        print_success("[FOUND] Node " + target + " returned: " + res->body);
    } else if (res && res->status == 404) {
        print_error("[404] Key not found on Node " + target);
    } else {
        std::string err = res ? std::to_string(res->status) : "Connection Refused";
        print_error("[FAIL] Could not read from Node " + target + " (Error: " + err + ")");
    }
}

int main() {
    ConsistentHashRing ring;

    // --- NO DEFAULT SERVERS ---
    // The ring starts empty. The user must add servers manually.

    print_info("--- Distributed KV Store Client ---");
    print_info("Status: Disconnected (0 Servers)");
    print_info("Commands:");
    print_info("  ADD <host>:<port>       (Connect to a server)");
    print_info("  SET <key> <value>       (Store data)");
    print_info("  GET <key>               (Retrieve data)");
    print_info("  EXIT                    (Quit)");
    std::cout << "-----------------------------------\n";

    std::string line, command, arg1, arg2;

    while (true) {
        std::cout << "> ";
        if (!std::getline(std::cin, line)) break;
        if (line.empty()) continue;

        std::stringstream ss(line);
        ss >> command;

        if (command == "EXIT" || command == "exit") {
            break;
        }
        else if (command == "ADD" || command == "add") {
            ss >> arg1;
            if (arg1.empty() || arg1.find(":") == std::string::npos) {
                print_error("Usage: ADD 127.0.0.1:<port>");
            } else {
                std::string fixed = sanitize_host(arg1);
                ring.addNode(fixed);
                print_success("[UPDATED] Added server: " + fixed);
            }
        }
        else if (command == "REMOVE" || command == "remove") {
            ss >> arg1;
            if (arg1.empty()) {
                print_error("Usage: REMOVE 127.0.0.1:<port>");
            } else {
                std::string fixed = sanitize_host(arg1);
                ring.removeNode(fixed);
                print_success("[UPDATED] Removed server: " + fixed);
            }
        }
        else if (command == "SET" || command == "set") {
            ss >> arg1;
            std::getline(ss, arg2);
            if (!arg2.empty() && arg2[0] == ' ') arg2 = arg2.substr(1);

            if (arg1.empty() || arg2.empty()) {
                print_error("Usage: SET <key> <value>");
            } else {
                put_data(ring, arg1, arg2);
            }
        }
        else if (command == "GET" || command == "get") {
            ss >> arg1;
            if (arg1.empty()) {
                print_error("Usage: GET <key>");
            } else {
                get_data(ring, arg1);
            }
        }
        else {
            print_error("Unknown command.");
        }
    }

    return 0;
}