cmake_minimum_required(VERSION 3.25)
project(kv_store VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── FetchContent dependencies ────────────────────────────────────────────────
include(FetchContent)

# Boost (header-only Asio + program_options)
FetchContent_Declare(
    Boost
    URL https://github.com/boostorg/boost/releases/download/boost-1.87.0/boost-1.87.0-cmake.tar.xz
    URL_HASH SHA256=7da75f171837577a52bbf217e17f8ea576c7c246e4594d617bfde7fafd408be5
    DOWNLOAD_EXTRACT_TIMESTAMP ON
)
set(BOOST_INCLUDE_LIBRARIES asio program_options system thread)
set(BOOST_ENABLE_CMAKE ON)

# spdlog
FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.15.1
    GIT_SHALLOW    TRUE
)

# Google Test
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.16.0
    GIT_SHALLOW    TRUE
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)

# protobuf
FetchContent_Declare(
    protobuf
    GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
    GIT_TAG        v29.3
    GIT_SHALLOW    TRUE
)
set(protobuf_BUILD_TESTS          OFF CACHE BOOL "" FORCE)
set(protobuf_BUILD_SHARED_LIBS    OFF CACHE BOOL "" FORCE)
set(protobuf_ABSL_PROVIDER        "package" CACHE STRING "" FORCE)

# abseil (required by protobuf v29+)
FetchContent_Declare(
    abseil
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG        20240722.1
    GIT_SHALLOW    TRUE
)
set(ABSL_ENABLE_INSTALL ON CACHE BOOL "" FORCE)
set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(abseil protobuf Boost spdlog googletest)

# ── Sanitizers (optional, requires libasan/libubsan) ─────────────────────────
option(KV_ENABLE_SANITIZERS "Enable ASan + UBSan (requires libasan)" OFF)

# ── Compiler flags ────────────────────────────────────────────────────────────
add_library(kv_compile_options INTERFACE)
target_compile_options(kv_compile_options INTERFACE
    -Wall
    -Wextra
    -Wpedantic
    -Wno-unused-parameter
    $<$<CONFIG:Debug>:-g -O0>
    $<$<CONFIG:Release>:-O2 -DNDEBUG>
    $<$<BOOL:${KV_ENABLE_SANITIZERS}>:-fsanitize=address,undefined>
)
target_link_options(kv_compile_options INTERFACE
    $<$<BOOL:${KV_ENABLE_SANITIZERS}>:-fsanitize=address,undefined>
)

# ── Subdirectories ────────────────────────────────────────────────────────────
add_subdirectory(src)
add_subdirectory(tests)
