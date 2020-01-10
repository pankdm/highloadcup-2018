#pragma once

#include "http_helpers.h"
#include "http_parser.h"
#include "http_structs.h"
#include "qs_parse.h"

#include "Base.h"
#include "Server.h"

struct HttpData {
    http_request req;
    http_response res;
    unsigned int method;
    const char* url = nullptr;
    size_t url_length = 0;
};

int OnUrl(http_parser* parser, const char* at, size_t length) {
    HttpData* http_data = (HttpData*)(parser->data);
    http_data->url = at;
    http_data->url_length = length;
    return 0;
}

int OnBody(http_parser* parser, const char* at, size_t length) {
    HttpData* http_data = (HttpData*)(parser->data);
    http_data->req.body = at;
    http_data->req.body_length = length;
    return 0;
}

class EpollServerAdapter {
   public:
    Server* server;
    std::atomic<int32_t> numRequests{0};
    explicit EpollServerAdapter(Server* server_) : server(server_) {}

    void getRequestParams(RequestParams& params, const http_request& req) {
        for (int i = 0; i < req.params_size; ++i) {
            std::string key = req.url_params[i].key;
            std::string value = req.url_params[i].val;
            params[key] = value;
        }
    }

    void filterAPI(const http_request& req, http_response& res) {
        MY_LOG(INFO_LEVEL, "handle filter API");
        bumpTotalRequests();

        RequestParams params;
        getRequestParams(params, req);

        json j;
        try {
            int32_t limit = extractLimitParam(params);
            server->filterAPI(j, params, limit);
        } catch (std::runtime_error& e) {
            MY_LOG(INFO_LEVEL, "Got error: \"" << e.what() << "\" while processing filter API");
            BadRequest400(HTTP_GET, res);
            return;
        }
        replyWithResponse(j, res);
    }

    void groupAPI(const http_request& req, http_response& res) {
        MY_LOG(INFO_LEVEL, "handle group API");
        bumpTotalRequests();

        RequestParams params;
        getRequestParams(params, req);

        json j;
        try {
            int32_t limit = extractLimitParam(params);
            server->groupAPI(j, params, limit);
        } catch (UnsupportedException& e) {
            MY_LOG(ERROR_LEVEL,
                   "UnsupportedException: \"" << e.what() << "\" while processing group API");
            BadRequest400(HTTP_GET, res);
            return;
        } catch (std::runtime_error& e) {
            MY_LOG(INFO_LEVEL, "Got error: \"" << e.what() << "\" while processing group API");
            BadRequest400(HTTP_GET, res);
            return;
        }
        replyWithResponse(j, res);
    }

    void recommendAPI(const http_request& req, http_response& res, AccountId accountId) {
        MY_LOG(INFO_LEVEL, "handle recommend API");
        bumpTotalRequests();

        if (!server->accountExists(accountId)) {
            NotFound404(HTTP_GET, res);
            return;
        }

        RequestParams params;
        getRequestParams(params, req);

        json j;
        try {
            int32_t limit = extractLimitParam(params);
            server->recommendAPI(j, accountId, limit, params);
        } catch (std::runtime_error& e) {
            MY_LOG(INFO_LEVEL, "Got error: \"" << e.what() << "\" while processing recommend API");
            BadRequest400(HTTP_GET, res);
            return;
        }
        replyWithResponse(j, res);
    }

    void suggestAPI(const http_request& req, http_response& res, AccountId accountId) {
        MY_LOG(INFO_LEVEL, "handle suggest API");
        bumpTotalRequests();

        if (!server->accountExists(accountId)) {
            NotFound404(HTTP_GET, res);
            return;
        }

        RequestParams params;
        getRequestParams(params, req);

        json j;
        try {
            int32_t limit = extractLimitParam(params);
            server->suggestAPI(j, accountId, limit, params);
        } catch (std::runtime_error& e) {
            MY_LOG(INFO_LEVEL, "Got error: \"" << e.what() << "\" while processing suggest API");
            BadRequest400(HTTP_GET, res);
            return;
        }
        replyWithResponse(j, res);
    }

    void likesAPI(const http_request& req, http_response& res) {
        bool ok = true;

        try {
            json j = json::parse(req.body, req.body + req.body_length);
            ok = server->likesAPI(j);
        } catch (std::exception& e) {
            MY_LOG(INFO_LEVEL, "Got error: \"" << e.what() << "\" while processing likes API");
            ok = false;
        }
        if (ok) {
            SendOK_202(res);
        } else {
            BadRequest400(HTTP_POST, res);
        }
    }

    void replyWithResponse(const json& j, http_response& res) {
        std::string j_str = j.dump();
        sprintf(res.response_buf, "%s", j_str.c_str());

        iovec* iov = res.iov;
        res.iov_size = 4;

        iov[0].iov_base = (void*)HEADER;
        iov[0].iov_len = HEADER_LEN;

        auto body_size = j_str.size();
        auto body_size_buf_len = sprintf(res.body_size_buf, "%u", (uint32_t)body_size);

        iov[1].iov_base = (void*)res.body_size_buf;
        iov[1].iov_len = body_size_buf_len;

        iov[2].iov_base = (void*)HEADER_BODY_SEPARATOR;
        iov[2].iov_len = HEADER_BODY_SEPARATOR_LEN;

        iov[3].iov_base = (void*)res.response_buf;
        iov[3].iov_len = body_size;
    }

    void bumpTotalRequests() {
        ++numRequests;
        if (numRequests % REQUESTS_LOGGING_RATE == 0) {
            std::cout << "numRequests = " << numRequests << " used memory = " << getUsedMemoryInMb()
                      << " Mb" << std::endl;
        }
    }
};

#define STRINGS_EQUAL(A, B) (!memcmp(A, B, sizeof(B)))

// NotFound = 404
// BadRequest = 400

void Route(EpollServerAdapter* adapter, HttpData& data) {
    char* url_str = (char*)data.url;
    url_str[data.url_length] = '\0';
    MY_LOG(INFO_LEVEL, "");
    MY_LOG(INFO_LEVEL, "got request: " << url_str);
    MY_LOG(INFO_LEVEL, "sizeof(users) = " << sizeof("users"));
    ;

    yuarel yua;
    int ret = yuarel_parse(&yua, url_str);
    if (ret < 0) {
        BadRequest400(data.method, data.res);
        return;
    }
    char* parts[MAX_PATH_SIZE];
    ret = yuarel_split_path(yua.path, parts, MAX_PATH_SIZE);
    int num_parts = ret;
    MY_LOG(INFO_LEVEL, "split path num_parts = " << num_parts);

    if (ret <= 0) {
        BadRequest400(data.method, data.res);
        return;
    }
    if (LOGGING_LEVEL >= INFO_LEVEL) {
        for (int i = 0; i < ret; ++i) {
            std::cout << i << " parts = " << parts[i] << std::endl;
        }
    }

    if (!STRINGS_EQUAL(parts[0], "accounts")) {
        NotFound404(data.method, data.res);
        return;
    }
    if (!yua.query) {
        BadRequest400(data.method, data.res);
        return;
    }

    uint32_t id = 0;
    if (data.method == HTTP_GET) {
        if (num_parts <= 1 || num_parts >= 4) {
            NotFound404(data.method, data.res);
            return;
        }

        if (yua.query) {
            ret = yuarel_parse_query(yua.query, '&', data.req.url_params, MAX_QUERY_SIZE);
            if (ret < 0) {
                BadRequest400(data.method, data.res);
                return;
            }
            data.req.params_size = ret;
            for (int i = 0; i < ret; ++i) {
                qs_decode(data.req.url_params[i].val);
                MY_LOG(INFO_LEVEL, "key = " << data.req.url_params[i].key
                                            << ", value = " << data.req.url_params[i].val);
            }
        }
        if (num_parts == 2) {
            if (STRINGS_EQUAL(parts[1], "filter")) {
                adapter->filterAPI(data.req, data.res);
                // get_handlers::GetUsers(data.req, data.res, id);
                // void GetUsers(const http_request& req, http_response& res, uint32_t id) {
                return;
            } else if (STRINGS_EQUAL(parts[1], "group")) {
                adapter->groupAPI(data.req, data.res);
                return;
            } else {
                NotFound404(data.method, data.res);
                return;
            }
        } else if (num_parts == 3) {
            if (!GetUint32(parts[1], &id)) {
                NotFound404(data.method, data.res);
                return;
            }
            if (STRINGS_EQUAL(parts[2], "recommend")) {
                adapter->recommendAPI(data.req, data.res, id);
                return;
            } else if (STRINGS_EQUAL(parts[2], "suggest")) {
                adapter->suggestAPI(data.req, data.res, id);
                return;
            } else {
                NotFound404(data.method, data.res);
                return;
            }
        }
    } else if (data.method == HTTP_POST) {
        if (num_parts == 2) {
            if (STRINGS_EQUAL(parts[1], "new")) {
                // post_handlers::UpdateUsers(data.req, data.res, id);
                // if (!User::Parse(req.body, req.body_length, &new_user, &update_flags)) {
                // bool User::Parse(const char* data, size_t length, User* user, int* update_flags,
                // uint32_t* id) {
                // TODO: validate new
                SendOK_201(data.res);
                return;
            } else if (STRINGS_EQUAL(parts[1], "likes")) {
                // TODO: validate likes
                adapter->likesAPI(data.req, data.res);
                return;
            } else {
                if (!GetUint32(parts[1], &id)) {
                    NotFound404(data.method, data.res);
                    return;
                }
                // TODO: validate update
                SendOK_202(data.res);
                return;
            }
        } else {
            NotFound404(data.method, data.res);
            return;
        }
    }
    NotFound404(data.method, data.res);
}
