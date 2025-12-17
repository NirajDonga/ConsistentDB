#include "../../include/httplib.h"
#include <iostream>
#include <string>
#include <unordered_map>
#include <mutex>

// Simple In-Memory DB
std::unordered_map<std::string, std::string> db;
std::mutex db_mutex;

int main(int argc, char* argv[]) {
    // Basic argument check
    if (argc != 2) {
        std::cerr << "Usage: ./kv_server <PORT>" << std::endl;
        return 1;
    }

    int port = std::atoi(argv[1]);
    httplib::Server svr;

    // --- WRITE API ---
    svr.Post("/put", [](const httplib::Request& req, httplib::Response& res) {
        std::string key = req.get_param_value("key");
        std::string val = req.get_param_value("val");

        {
            std::lock_guard<std::mutex> lock(db_mutex);
            db[key] = val;
        }
        std::cout << "[Storage " << req.remote_port << "] Saved: " << key << std::endl;
        res.set_content("OK", "text/plain");
    });

    // --- READ API ---
    svr.Get("/get", [](const httplib::Request& req, httplib::Response& res) {
        std::string key = req.get_param_value("key");

        std::lock_guard<std::mutex> lock(db_mutex);
        if (db.count(key)) {
            res.set_content(db[key], "text/plain");
        } else {
            res.status = 404;
            res.set_content("Not Found", "text/plain");
        }
    });

    std::cout << "Starting Node on Port " << port << "..." << std::endl;
    svr.listen("0.0.0.0", port);
}