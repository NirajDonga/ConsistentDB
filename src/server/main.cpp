#include "httplib.h"
#include <iostream>
#include <string>
#include <unordered_map>
#include <mutex>
#include <vector>
#include <sstream>

using namespace std;

// In-Memory Storage
const int NUM_SHARDS = 16;
unordered_map<string, string> db_shards[NUM_SHARDS];
mutex shard_mutexes[NUM_SHARDS];

size_t get_shard_id(const string& key) {
    return hash<string>{}(key) % NUM_SHARDS;
}

// Helper: Check if hash 'h' is inside (start, end] handling wrap-around
bool in_range(size_t h, size_t start, size_t end) {
    if (start < end) return h > start && h <= end;
    return h > start || h <= end; // Wrap around case
}

int main(int argc, char* argv[]) {
    if (argc != 2) { cerr << "Usage: ./kv_server <PORT>" << endl; return 1; }
    int port = atoi(argv[1]);

    // --- CRITICAL FIX: Disable buffering so logs appear INSTANTLY ---
    std::cout.setf(std::ios::unitbuf);

    httplib::Server svr;

    // WRITE
    svr.Post("/put", [](const httplib::Request& req, httplib::Response& res) {
        string key = req.get_param_value("key");
        string val = req.get_param_value("val");
        int id = get_shard_id(key);
        { lock_guard<mutex> lock(shard_mutexes[id]); db_shards[id][key] = val; }

        // GREEN log for Adds
        cout << "\033[1;32m[Storage] Saved: " << key << "\033[0m" << endl;
        res.set_content("OK", "text/plain");
    });

    // DELETE
    svr.Post("/del", [](const httplib::Request& req, httplib::Response& res) {
        string key = req.get_param_value("key");
        int id = get_shard_id(key);
        { lock_guard<mutex> lock(shard_mutexes[id]); db_shards[id].erase(key); }

        // RED log for Deletes (Force flush)
        cout << "\033[1;31m[Storage] Deleted: " << key << "\033[0m" << endl;
        res.set_content("OK", "text/plain");
    });

    // READ
    svr.Get("/get", [](const httplib::Request& req, httplib::Response& res) {
        string key = req.get_param_value("key");
        int id = get_shard_id(key);
        lock_guard<mutex> lock(shard_mutexes[id]);
        if (db_shards[id].count(key)) res.set_content(db_shards[id][key], "text/plain");
        else { res.status = 404; res.set_content("Not Found", "text/plain"); }
    });

    // --- OPTIMIZED RANGE EXPORT ---
    svr.Get("/range", [](const httplib::Request& req, httplib::Response& res) {
        if (!req.has_param("start") || !req.has_param("end")) { res.status = 400; return; }

        size_t start = stoull(req.get_param_value("start"));
        size_t end = stoull(req.get_param_value("end"));

        stringstream ss;
        for (int i = 0; i < NUM_SHARDS; ++i) {
            lock_guard<mutex> lock(shard_mutexes[i]);
            for (const auto& pair : db_shards[i]) {
                size_t h = hash<string>{}(pair.first);
                if (in_range(h, start, end)) {
                    ss << pair.first << "\n" << pair.second << "\n";
                }
            }
        }
        res.set_content(ss.str(), "text/plain");
    });

    // OLD: Full Export
    svr.Get("/all", [](const httplib::Request& req, httplib::Response& res) {
        stringstream ss;
        for (int i = 0; i < NUM_SHARDS; ++i) {
            lock_guard<mutex> lock(shard_mutexes[i]);
            for (const auto& pair : db_shards[i]) ss << pair.first << "\n" << pair.second << "\n";
        }
        res.set_content(ss.str(), "text/plain");
    });

    cout << "--- Optimized Server Port " << port << " (Logs Enabled) ---" << endl;
    svr.listen("0.0.0.0", port);
}