#include "constants.h"

#include <cstring>

const char* HEADER =
    "HTTP/1.1 200 OK\r\n"
    "S: b\r\n"
    "C: k\r\n"
    "B: a\r\n"
    "Content-Length: ";
const size_t HEADER_LEN = strlen(HEADER);

const char* HEADER_BODY_SEPARATOR = "\r\n\r\n";
const size_t HEADER_BODY_SEPARATOR_LEN = strlen(HEADER_BODY_SEPARATOR);
