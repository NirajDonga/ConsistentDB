cmake_minimum_required(VERSION 3.10)
project(DistributedKVStore)

set(CMAKE_CXX_STANDARD 17)

# --- FIX 1: Windows 10/11 Support (Fixes CreateFile2 error) ---
if(WIN32)
    add_definitions(-D_WIN32_WINNT=0x0A00)
endif()

# --- FIX 2: Static Linking (Fixes "Silent Crash" in terminal) ---
# This ensures all DLLs are included in the .exe so it runs anywhere
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -static")

include_directories(include)

# 1. The Shared Hash Ring Library
add_library(hash_ring src/common/hash_ring.cpp)

# 2. The Server (Storage Node)
add_executable(kv_server src/server/main.cpp)

# 3. The Client (Coordinator)
add_executable(kv_client src/client/main.cpp)

# Link Libraries
if(WIN32)
    # ws2_32 is required for networking on Windows
    target_link_libraries(kv_server hash_ring ws2_32 crypt32)
    target_link_libraries(kv_client hash_ring ws2_32 crypt32)
else()
    target_link_libraries(kv_server hash_ring pthread)
    target_link_libraries(kv_client hash_ring pthread)
endif()