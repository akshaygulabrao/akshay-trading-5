cmake_minimum_required(VERSION 3.16)

project(WebSocketClient)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2")
set(CMAKE_OSX_ARCHITECTURES "arm64")

find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(RapidJSON REQUIRED)

# Include Paths
include_directories(
    /opt/homebrew/include            # Homebrew general includes
    /usr/local/include               # General include directory
    /usr/local/include/ixwebsocket
)

link_directories(
    /usr/local/lib
)

# Define the source files for the project
add_executable(websocket_client src/main.cpp)

# Link libraries (explicit OpenSSL linking)
target_link_libraries(websocket_client
    PRIVATE
        ixwebsocket
        ZLIB::ZLIB
        OpenSSL::SSL
        OpenSSL::Crypto
)

if(APPLE)
    # macOS-specific libraries for IXWebSocket TLS support
    find_library(COREFOUNDATION_LIBRARY CoreFoundation)
    find_library(SECURITY_LIBRARY Security)

    target_link_libraries(websocket_client
        PRIVATE
            ${COREFOUNDATION_LIBRARY}
            ${SECURITY_LIBRARY}
    )
endif()
