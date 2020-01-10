#pragma once

#include <unistd.h>

constexpr int MAX_QUERY_SIZE = 10;
constexpr int MAX_PATH_SIZE = 10;

constexpr int MAX_IOVEC_SIZE = 10;
constexpr int MAX_IOVEC_PART = 1000;

constexpr size_t kReadBufferSize = 2048;

constexpr int MAX_RESPONSE_SIZE = 1024 * 64;

extern const char* HEADER;
extern const size_t HEADER_LEN;

extern const char* HEADER_BODY_SEPARATOR;
extern const size_t HEADER_BODY_SEPARATOR_LEN;
