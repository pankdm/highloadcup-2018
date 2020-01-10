#pragma once

#include "Base.h"
#include "MemoryUsage.h"
#include "Semaphore.h"
#include "Server.h"
#include "Util.h"

class RequestsInFlightCounter {
   public:
    RequestsInFlightCounter(std::atomic<int32_t>* counter) : _counter(counter) { ++(*_counter); }
    ~RequestsInFlightCounter() { --(*_counter); }

   private:
    std::atomic<int32_t>* _counter;
};

#define GET_REQUEST_PARAMS(params)                 \
    std::string query;                             \
    auto len = strlen(reqInfo->query_string);      \
    query.reserve(len);                            \
    urlDecode(reqInfo->query_string, query, true); \
    auto params = parseQuery(query);

#define RETURN_WITH_ERROR_404(conn) \
    replyWithCode(conn, 404, "");   \
    return true;

#define RETURN_WITH_ERROR_400(conn) \
    replyWithCode(conn, 400, "");   \
    return true;

class HttpHandler : public CivetHandler {
   public:
    HttpHandler(Server* server)
        : _server(server), bgThread(&HttpHandler::checkRebuild, std::ref(*this)) {}

    // std::string decodeQueryString(const std::string& queryString) {
    //     mg_url_decode(queryString.c_str(), queryString.size(), buf, BUFFER_SIZE, true);
    //     return buf;
    // }
    //
    void urlDecode(const char* src, std::string& dst, bool is_form_url_encoded) {
        urlDecode(src, strlen(src), dst, is_form_url_encoded);
    }

    void urlDecode(const char* src, size_t src_len, std::string& dst, bool is_form_url_encoded) {
        int i, j, a, b;
#define HEXTOI(x) (isdigit(x) ? x - '0' : x - 'W')

        dst.clear();
        for (i = j = 0; i < (int)src_len; i++, j++) {
            if (i < (int)src_len - 2 && src[i] == '%' && isxdigit((unsigned char)src[i + 1]) &&
                isxdigit((unsigned char)src[i + 2])) {
                a = tolower((unsigned char)src[i + 1]);
                b = tolower((unsigned char)src[i + 2]);
                dst.push_back((char)((HEXTOI(a) << 4) | HEXTOI(b)));
                i += 2;
            } else if (is_form_url_encoded && src[i] == '+') {
                dst.push_back(' ');
            } else {
                dst.push_back(src[i]);
            }
        }
    }

    bool handleGet(CivetServer* server, mg_connection* conn) {
        ++numGets;
        RequestsInFlightCounter scoped(&requestsInFlight);
        bumpTotalRequests();

        MY_LOG(DEBUG_LEVEL,
               "Waiting for request, count = " << _semaphore.getCounter()
                                               << " in flight = " << requestsInFlight.load());
        // printUsedMemory();
        ScopedSemaphore scope(_semaphore);

        auto reqInfo = mg_get_request_info(conn);
        std::string uri = reqInfo->request_uri;

        // check if there is rebuild in flight and abort if needed
        if (rebuildingIndex) {
          // MY_LOG(ALWAYS_LOG, "Still rebuilding, exiting");
          RETURN_WITH_ERROR_400(conn);
        }

        MY_LOG(INFO_LEVEL, "processing " << uri << reqInfo->query_string);
        // MY_LOG(INFO_LEVEL, "decoded = " << query);

        // MY_LOG(ALWAYS_LOG, "GET request: " << uri << reqInfo->query_string << ", verifying counters");
        // _server->verifyCountryCounter();
        if (uri == "/accounts/filter/") {
            // filter API call
            GET_REQUEST_PARAMS(params);
            processFilter(conn, params);
            return true;
        } else if (uri == "/accounts/group/") {
            // group API call
            GET_REQUEST_PARAMS(params);
            processGroup(conn, params);
            return true;
        } else {
            auto parts = splitString(uri, '/');
            if (parts.size() >= 5) {
                if (parts[3] == "recommend") {
                    // recommend API call
                    if (!isDigit(parts[2])) {
                        RETURN_WITH_ERROR_404(conn);
                    }
                    auto id = convert<AccountId>(parts[2]);
                    GET_REQUEST_PARAMS(params);
                    processRecommend(conn, id, params);
                    return true;
                } else if (parts[3] == "suggest") {
                    // suggest API call
                    if (!isDigit(parts[2])) {
                        RETURN_WITH_ERROR_404(conn);
                    }
                    auto id = convert<AccountId>(parts[2]);
                    GET_REQUEST_PARAMS(params);
                    processSuggest(conn, id, params);
                    return true;
                }
            }
        }
        RETURN_WITH_ERROR_404(conn);
    }

    bool handlePost(CivetServer* server, mg_connection* conn) {
        ++numPosts;

        bumpTotalRequests();
        ScopedSemaphore scope(_semaphore);

        auto reqInfo = mg_get_request_info(conn);
        std::string uri = reqInfo->request_uri;
        bool error = false;
        // MY_LOG(ALWAYS_LOG, "POST request: " << uri << reqInfo->query_string << ", verifying counters");
        // _server->verifyCountryCounter();
        if (uri == "/accounts/new/") {
            // new account API call
            auto data = getPostData(conn);
            auto j = json::parse(data);
            bool ok = _server->newAPI(j);
            if (ok) {
                replyWithCode(conn, 201, "{}");
                return true;
            } else {
                RETURN_WITH_ERROR_400(conn);
            }
        } else if (uri == "/accounts/likes/") {
            // likes API call
            auto data = getPostData(conn);
            auto j = json::parse(data);
            // std::cout << " running likes API" << std::endl;
            bool ok = _server->likesAPI(j);
            if (ok) {
                replyWithCode(conn, 202, "{}");
                return true;
            } else {
                RETURN_WITH_ERROR_400(conn);
            }
        } else {
            // update API call
            auto parts = splitString(uri, '/');
            if (parts.size() >= 3) {
                if (!isDigit(parts[2])) {
                    RETURN_WITH_ERROR_404(conn);
                }
                auto id = convert<AccountId>(parts[2]);
                processUpdate(conn, id);
                // _server->verifyCountryCounter();
                return true;
            } else {
                RETURN_WITH_ERROR_404(conn);
            }
        }
        RETURN_WITH_ERROR_404(conn);
    }

    void replyWithCode(mg_connection* conn, int code, const std::string& body) {
        // Status-Line:
        mg_printf(conn, "HTTP/1.1 %d %s\r\n", code, mg_get_response_code_text(conn, code));
        mg_printf(conn, "Content-Type: text/html\r\n");
        mg_printf(conn, "Content-Length: %lu\r\n", body.size());
        mg_printf(conn, "Server: C++/gcc-7.2\r\n");
        mg_printf(conn, "Connection: keep-alive\r\n");
        mg_printf(conn, "\r\n");
        mg_printf(conn, "%s", body.c_str());
    }

    void processRecommend(mg_connection* conn, AccountId accountId, const RequestParams& params) {
        if (!_server->accountExists(accountId)) {
            replyWithCode(conn, 404, "");
            return;
        }

        json j;
        try {
            int32_t limit = extractLimitParam(params);
            _server->recommendAPI(j, accountId, limit, params);
        } catch (std::runtime_error& e) {
            MY_LOG(INFO_LEVEL, "Got error: \"" << e.what() << "\" while processing recommend API");
            replyWithCode(conn, 400, "");
            return;
        }
        if (JSON_FAST_DUMP) {
            replyWithCode(conn, 200, j.dump());
        } else {
            replyWithCode(conn, 200, j.dump(4));
        }
    }

    void processSuggest(mg_connection* conn, AccountId accountId, const RequestParams& params) {
        // for (const auto& [k, v] : params) {
        //     std::cout << "param: " << k << " --> " << v << std::endl;
        // }
        if (!_server->accountExists(accountId)) {
            replyWithCode(conn, 404, "");
            return;
        }
        json j;
        try {
            int32_t limit = extractLimitParam(params);
            _server->suggestAPI(j, accountId, limit, params);
        } catch (std::runtime_error& e) {
            MY_LOG(INFO_LEVEL, "Got error: \"" << e.what() << "\" while processing suggest API");
            replyWithCode(conn, 400, "");
            return;
        }
        if (JSON_FAST_DUMP) {
            replyWithCode(conn, 200, j.dump());
        } else {
            replyWithCode(conn, 200, j.dump(4));
        }
    }

    void processFilter(mg_connection* conn, const RequestParams& params) {
        json j;
        try {
            int32_t limit = extractLimitParam(params);
            _server->filterAPI(j, params, limit);
        } catch (std::runtime_error& e) {
            MY_LOG(INFO_LEVEL, "Got error: \"" << e.what() << "\" while processing filter API");
            replyWithCode(conn, 400, "");
        }
        if (JSON_FAST_DUMP) {
            replyWithCode(conn, 200, j.dump());
        } else {
            replyWithCode(conn, 200, j.dump(4));
        }
    }

    void processGroup(mg_connection* conn, const RequestParams& params) {
        // construct response
        // std::vector<AccountId> ids;
        json j;
        try {
            int32_t limit = extractLimitParam(params);
            _server->groupAPI(j, params, limit);
        } catch (UnsupportedException& e) {
            MY_LOG(ERROR_LEVEL,
                   "UnsupportedException: \"" << e.what() << "\" while processing group API");
            replyWithCode(conn, 400, "");
        } catch (std::runtime_error& e) {
            MY_LOG(INFO_LEVEL, "Got error: \"" << e.what() << "\" while processing group API");
            replyWithCode(conn, 400, "");
        }
        if (JSON_FAST_DUMP) {
            replyWithCode(conn, 200, j.dump());
        } else {
            replyWithCode(conn, 200, j.dump(4));
        }
    }

    void processUpdate(mg_connection* conn, AccountId id) {
        if (!_server->accountExists(id)) {
            replyWithCode(conn, 404, "");
            return;
        }

        bool ok = false;
        try {
            auto data = getPostData(conn);
            auto j = json::parse(data);
            ok = _server->updateAPI(id, j);
            // std::cout << " running likes API" << std::endl;
        } catch (std::runtime_error& e) {
            MY_LOG(INFO_LEVEL, "Got error: \"" << e.what() << "\" while processing update API");
            ok = false;
        }
        if (ok) {
            replyWithCode(conn, 202, "{}");
        } else {
            replyWithCode(conn, 400, "");
        }
    }

    void bumpTotalRequests() {
        ++numRequests;

        if (ENABLE_MEMORY_CHECKING) {
            auto now = getUsedMemory();
            auto nowMb = now / 1000 / 1000;
            auto limit = totalMemory.load();
            if (nowMb > limit + 20) {
                totalMemory = nowMb;
                std::cout << numRequests << ": last memory = " << limit << "Mb, now = " << nowMb
                          << "Mb, in flight requests = " << requestsInFlight.load() << std::endl;
            }
        }

        if (numRequests % REQUESTS_LOGGING_RATE == 0) {
            std::cout << "numRequests = " << numRequests << " used memory = " << getUsedMemoryInMb()
                      << " Mb" << std::endl;
            // printUsedMemory();
            // _server->printIndexStats();
        }
    }

    // void doRecommendForRandomUser(mg_connection* conn) {
    //     int random = std::rand();
    //     int size = _server->index.allAccounts.size();
    //     auto accountId = _server->index.allAccounts[random % size];
    //     RequestParams params;
    //     params["limit"] = 50;
    //     auto before = getUsedMemory();
    //     processRecommend(conn, accountId, params);
    //     auto after = getUsedMemory();
    //     auto afterMb = (after / 1000) / 1000.;
    //     auto beforeMb = (before / 1000) / 1000.;
    //     auto limit = totalMemory.load();
    //     if (afterMb > limit + 10) {
    //         totalMemory = afterMb;
    //         std::cout << numRequests << ": recommend for user " << accountId << " last = " <<
    //         limit
    //                   << "Mb now = " << afterMb << " Mb before = " << beforeMb << " Mb, "
    //                   << "in flight = " << requestsInFlight.load() << std::endl;
    //     }
    // }

    std::string getPostData(struct mg_connection* conn) {
        std::string postdata;
        char buf[2048];
        int r = mg_read(conn, buf, sizeof(buf));
        while (r > 0) {
            postdata += std::string(buf, r);
            r = mg_read(conn, buf, sizeof(buf));
        }
        return postdata;
    };

    void checkRebuild() {
        int32_t prevNumPosts = numPosts.load();
        Timer t;
        t.start();
        bool needRebuild = false;
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (numPosts > prevNumPosts) {
                prevNumPosts = numPosts.load();
                needRebuild = true;
                t.start();
                continue;
            }
            if (needRebuild && t.elapsedMilliseconds() > REBUILD_TIMEOUT_MS) {
                MY_LOG(ALWAYS_LOG, "#POSTS = " << prevNumPosts << "; No new requests after "
                                               << t.elapsedMilliseconds() << " ms, rebuilding");
                prevNumPosts = numPosts.load();
                Timer tr;
                tr.start();

                RequestsInFlightCounter scoped(&rebuildingIndex);
                _server->rebuildIndexes();

                // MY_LOG(ALWAYS_LOG, "Sleeping for 5s");
                // std::this_thread::sleep_for(std::chrono::milliseconds(5000));

                MY_LOG(ALWAYS_LOG, "Rebuild finished in " << tr.elapsedMilliseconds() << " ms");
                needRebuild = false;
                continue;
            }
        }
    }

   private:
    Server* _server;
    // static constexpr size_t BUFFER_SIZE = 4096;
    // char buf[BUFFER_SIZE];

    std::atomic<int32_t> numRequests{0};
    std::atomic<int32_t> numGets{0};
    std::atomic<int32_t> numPosts{0};

    std::atomic<int32_t> requestsInFlight{0};

    std::atomic<int32_t> rebuildingIndex{0};

    Semaphore _semaphore{NUM_CONCURRENT_REQUESTS};

    std::atomic<int64_t> totalMemory{0};

    std::thread bgThread;
};
