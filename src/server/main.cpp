#include "httplib.h"
#include <iostream>
#include <string>
#include <unordered_map>
#include <mutex>
#include <vector>
#include <fstream>
#include <sstream>

using namespace std;

// --- FEATURE 2: Sharded Locking ---
const int NUM_SHARDS = 16;
unordered_map<string, string> db_shards[NUM_SHARDS];
mutex shard_mutexes[NUM_SHARDS];

// --- FEATURE 1: Persistence (WAL) ---
mutex log_mutex;
string log_file_name;

size_t get_shard_id(const string& key) {
    return hash<string>{}(key) % NUM_SHARDS;
}

// Log command to disk
void wal_log(const string& key, const string& val) {
    lock_guard<mutex> lock(log_mutex);
    ofstream file(log_file_name, ios::app); // Append mode
    file << "SET " << key << " " << val << "\n";
}

// Replay log on startup
void wal_recover() {
    ifstream file(log_file_name);
    if (!file.is_open()) return;

    cout << "[WAL] Recovering data from " << log_file_name << "..." << endl;
    string line, cmd, key, val;
    int count = 0;

    while (getline(file, line)) {
        stringstream ss(line);
        ss >> cmd >> key;
        getline(ss, val);

        if (!val.empty()) val = val.substr(1); // Remove leading space

        if (cmd == "SET") {
            int id = get_shard_id(key);
            db_shards[id][key] = val; // No lock needed during single-threaded startup
            count++;
        }
    }
    cout << "[WAL] Recovered " << count << " records." << endl;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        cerr << "Usage: ./kv_server <PORT>" << endl;
        return 1;
    }

    int port = atoi(argv[1]);
    log_file_name = "data_" + to_string(port) + ".log"; // Unique log per port

    // 1. Recover data before starting server
    wal_recover();

    httplib::Server svr;

    // --- WRITE API ---
    svr.Post("/put", [](const httplib::Request& req, httplib::Response& res) {
        string key = req.get_param_value("key");
        string val = req.get_param_value("val");

        // 1. Write to Disk (Durability)
        wal_log(key, val);

        // 2. Write to Memory (Sharded Lock)
        int id = get_shard_id(key);
        {
            lock_guard<mutex> lock(shard_mutexes[id]);
            db_shards[id][key] = val;
        }

        cout << "[Storage " << req.remote_port << "] Saved: " << key << endl;
        res.set_content("OK", "text/plain");
    });

    // --- READ API ---
    svr.Get("/get", [](const httplib::Request& req, httplib::Response& res) {
        string key = req.get_param_value("key");

        int id = get_shard_id(key);
        lock_guard<mutex> lock(shard_mutexes[id]);

        if (db_shards[id].count(key)) {
            res.set_content(db_shards[id][key], "text/plain");
        } else {
            res.status = 404;
            res.set_content("Not Found", "text/plain");
        }
    });

    cout << "Starting Node on Port " << port << "..." << endl;
    svr.listen("0.0.0.0", port);
}