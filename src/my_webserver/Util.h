#pragma once

#include "Base.h"

#include <dirent.h>
#include <sys/types.h>

#include <cstdio>
#include <cstdlib>

#include <sys/stat.h>
#include "unistd.h"

std::vector<std::string> splitString(const std::string& str, char delim) {
    std::vector<std::string> cont;
    std::size_t current, previous = 0;
    current = str.find(delim);
    while (current != std::string::npos) {
        cont.push_back(str.substr(previous, current - previous));
        previous = current + 1;
        current = str.find(delim, previous);
    }
    cont.push_back(str.substr(previous, current - previous));
    return cont;
}

std::unordered_map<std::string, std::string> parseQuery(const std::string& query) {
    std::unordered_map<std::string, std::string> result;
    auto parts = splitString(query, '&');
    for (const auto& part : parts) {
        auto kv = splitString(part, '=');
        if (kv.size() >= 2) {
            result[kv[0]] = kv[1];
        }
    }
    return result;
}

bool isDigit(const std::string& s) {
    if (s.empty()) {
        return false;
    }
    for (char c : s) {
        if (c < '0' || c > '9') {
            return false;
        }
    }
    return true;
}

template <class T>
T convert(const std::string& str) {
    std::stringstream s;
    s << str;
    T output;
    s >> output;
    return output;
}

bool endsWith(const std::string& s, const std::string& suffix) {
    if (s.size() < suffix.size()) {
        return false;
    }
    auto substr = s.substr(s.size() - suffix.size(), suffix.size());
    return substr == suffix;
}

bool startsWith(const std::string& s, const std::string& prefix) {
    if (s.size() < prefix.size()) {
        return false;
    }
    auto substr = s.substr(0, prefix.size());
    return substr == prefix;
}

void readDirectory(const std::string& name, std::vector<std::string>* v) {
    DIR* dirp = opendir(name.c_str());
    struct dirent* dp;
    while ((dp = readdir(dirp)) != NULL) {
        v->push_back(dp->d_name);
    }
    closedir(dirp);
}

bool fileExists(const std::string& filename) {
    struct stat buffer;
    return stat(filename.c_str(), &buffer) == 0;
}

// helper function to print a tuple of any size
template <class Tuple, std::size_t N>
struct TuplePrinter {
    static void print(const Tuple& t) {
        TuplePrinter<Tuple, N - 1>::print(t);
        std::cout << ", " << std::get<N - 1>(t);
    }
};

template <class Tuple>
struct TuplePrinter<Tuple, 1> {
    static void print(const Tuple& t) { std::cout << std::get<0>(t); }
};

template <class... Args>
void tuplePrint(const std::tuple<Args...>& t) {
    std::cout << "(";
    TuplePrinter<decltype(t), sizeof...(Args)>::print(t);
    std::cout << ")\n";
}
// end helper function

namespace stl {
/**
 * Given a map and a key, return a pointer to the value corresponding to the
 * key in the map, or nullptr if the key doesn't exist in the map.
 */
template <class Map, typename Key = typename Map::key_type>
const typename Map::mapped_type* mapGetPtr(const Map& map, const Key& key) {
    auto pos = map.find(key);
    return (pos != map.end() ? &pos->second : nullptr);
}

/**
 * Non-const overload of the above.
 */
template <class Map, typename Key = typename Map::key_type>
typename Map::mapped_type* mapGetPtr(Map& map, const Key& key) {
    auto pos = map.find(key);
    return (pos != map.end() ? &pos->second : nullptr);
}

template <class Map, typename Key = typename Map::key_type>
bool contains(Map& map, const Key& key) {
    auto pos = map.find(key);
    return pos != map.end();
}

}  // namespace stl
