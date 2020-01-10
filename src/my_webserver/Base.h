#pragma once

#include <time.h> /* time_t, struct tm, time, gmtime */
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "json.hpp"
using json = nlohmann::json;

#include "CivetServer.h"

constexpr int NOTHING_LEVEL = -1;

constexpr int ALWAYS_LOG = -2;
constexpr int ERROR_LEVEL = 0;
constexpr int INFO_LEVEL = 1;
constexpr int DEBUG_LEVEL = 2;

// constexpr bool ENABLE_DEBUG_REQUESTS = true;
constexpr bool LOAD_TEST_DATA = false;
constexpr bool LOAD_LIKES_INDEX = true;

constexpr int32_t REBUILD_TIMEOUT_MS = 1200;  // 1.2s

// constexpr int LOGGING_LEVEL = INFO_LEVEL;
// constexpr int LOGGING_LEVEL = ERROR_LEVEL;

#if defined(USE_PRODUCTION_SETTINGS)
// using the following settings while debugging memory leak
// constexpr bool ENABLE_RECOMMEND_API = false;
// constexpr bool DEBUG_MEMORY_LEAK = true;
// constexpr int RANDOM_RECOMMEND_RATE = 1;
constexpr int REQUESTS_LOGGING_RATE = 4000;
constexpr int LOGGING_LEVEL = ERROR_LEVEL;

constexpr bool ENABLE_RECOMMEND_API = true;
constexpr bool DEBUG_MEMORY_LEAK = false;
constexpr bool RANDOM_RECOMMEND_RATE = 0;

constexpr bool ENABLE_MEMORY_CHECKING = false;
constexpr int NUM_CONCURRENT_REQUESTS = 4;

constexpr bool USE_FAST_HTTP_SERVER = false;
constexpr bool JSON_FAST_DUMP = true;

#define THROW_ERROR(msg) throw std::runtime_error("");

#else
// constexpr int LOGGING_LEVEL = NOTHING_LEVEL;
constexpr int LOGGING_LEVEL = ERROR_LEVEL;

constexpr int REQUESTS_LOGGING_RATE = 1000;

constexpr bool ENABLE_RECOMMEND_API = true;
constexpr bool DEBUG_MEMORY_LEAK = false;

constexpr int RANDOM_RECOMMEND_RATE = 0;
constexpr bool ENABLE_MEMORY_CHECKING = false;
constexpr int NUM_CONCURRENT_REQUESTS = 1;

// constexpr bool USE_FAST_HTTP_SERVER = true;
constexpr bool USE_FAST_HTTP_SERVER = false;

constexpr bool JSON_FAST_DUMP = false;

#define THROW_ERROR(msg) throw std::runtime_error(msg);
// #define THROW_ERROR(msg) throw std::runtime_error("");

#endif

#define STR(x) #x
#define MY_ASSERT(x)                                                        \
    if (!(x)) {                                                             \
        printf(                                                             \
            "My custom assertion failed: (%s), function %s, file %s, line " \
            "%d.\n",                                                        \
            STR(x), __PRETTY_FUNCTION__, __FILE__, __LINE__);               \
        abort();                                                            \
    }

#define MY_ASSERT_EQ(x, y)                                                        \
    if (!(x == y)) {                                                              \
        printf(                                                                   \
            "My custom assertion failed: (%s == %s), function %s, file %s, line " \
            "%d.\n",                                                              \
            STR(x), STR(y), __PRETTY_FUNCTION__, __FILE__, __LINE__);             \
        std::cout << "Expected: " << y << ", got: " << x << std::endl;            \
        abort();                                                                  \
    }

char getLoggingPrefix(int level) {
    switch (level) {
        case ERROR_LEVEL:
            return 'E';
        case INFO_LEVEL:
            return 'I';
        case DEBUG_LEVEL:
            return 'D';
        default:
            return '?';
    }
}

#define MY_LOG(level, x)                                                                       \
    if (level <= LOGGING_LEVEL) {                                                              \
        printf("%c| %.3fs | [%s:%d] ", getLoggingPrefix(level), GLOBAL_TIMER.elapsedSeconds(), \
               __FILE__, __LINE__);                                                            \
        std::cout << x << std::endl;                                                           \
    }

#define MY_LOG_WITH_MEMORY(x)                                                                   \
    if (true) {                                                                                 \
        auto usedMemory = getUsedMemoryInMb();                                                  \
        printf("?| %.3fs | [%s:%d] Mem = %.2lf Mb | ", GLOBAL_TIMER.elapsedSeconds(), __FILE__, \
               __LINE__, usedMemory);                                                           \
        std::cout << x << std::endl;                                                            \
    }

#define SORT_REVERSE(a) std::sort(a.rbegin(), a.rend());
#define SORT_REVERSE_WITH_LAMBDA(a, lambda) std::sort(a.rbegin(), a.rend(), lambda);

// #define FOLLY_NO_CONFIG
// #include <folly/sorted_vector_types.h>

class UnsupportedException : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

namespace constants {
constexpr double EPSILON = 1e-6;
}

//  on why XOR is not a good choice for hash-combining:
//  https://stackoverflow.com/questions/5889238/why-is-xor-the-default-way-to-combine-hashes
//
//  this is from boost
//
template <typename T>
inline void hash_combine(std::size_t& seed, const T& val) {
    std::hash<T> hasher;
    seed ^= hasher(val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

//  taken from https://stackoverflow.com/a/7222201/916549
//
template <typename S, typename T>
struct std::hash<std::pair<S, T>> {
    inline size_t operator()(const std::pair<S, T>& val) const {
        size_t seed = 0;
        hash_combine(seed, val.first);
        hash_combine(seed, val.second);
        return seed;
    }
};
