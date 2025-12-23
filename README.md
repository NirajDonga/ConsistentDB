
Markdown

# Distributed Key-Value Store

This is a C++ project demonstrating **Consistent Hashing** with automatic data migration and dynamic scaling.

## Getting Started

First, build the project using CMake:

```bash
mkdir build && cd build
cmake ..
cmake --build .
Next, start your servers (in separate terminals):

Bash

# Terminal 1
./kv_server 8080

# Terminal 2
./kv_server 8081
Finally, run the client application:

Bash

./kv_client
Usage
Once the client is running, you can interact with the cluster:

ADD 127.0.0.1:8081 - Add a server and trigger optimized rebalancing.

REMOVE 127.0.0.1:8080 - Remove a server and evacuate data.

SET <key> <val> - Store a value.

GET <key> - Retrieve a value.