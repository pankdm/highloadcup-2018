#pragma once

#include "Base.h"

#include "Route.h"

#include "http_helpers.h"
#include "http_parser.h"
#include "http_structs.h"

#include "Server.h"

#ifdef __linux__
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <thread>

void runEpollServer(Server *server, std::string portStr) {
    int port = convert<int32_t>(portStr);

    EpollServerAdapter adapter(server);
    auto adapterPtr = &adapter;

    int listener;
    struct sockaddr_in addr;
    int epollfd = 0;
    int max_events = 1000;

    signal(SIGPIPE, SIG_IGN);

    listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) {
        perror("socket");
        exit(1);
    }

    int i = 1;
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, (void *)&i, sizeof(i));
    setsockopt(listener, IPPROTO_TCP, TCP_QUICKACK, (void *)&i, sizeof(i));
    setsockopt(listener, IPPROTO_TCP, TCP_DEFER_ACCEPT, (void *)&i, sizeof(i));

    addr.sin_family = AF_INET;
    // addr.sin_port = htons(8080);
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(listener, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        exit(2);
    }

    listen(listener, 100);

    epollfd = epoll_create1(0);
    if (epollfd == -1) {
        perror("epoll_create1");
        exit(EXIT_FAILURE);
    }

    std::vector<std::thread> threads;
    unsigned int thread_nums = 4;
    for (unsigned int i = 0; i < thread_nums; ++i) {
        threads.push_back(std::thread([epollfd, max_events, i, adapterPtr]() {
            // while(true) {
            //     std::cout << "sleeping 1s in thread " << i << std::endl;
            //     sleep(1);
            // }
            epoll_event events[max_events];
            http_parser_settings settings;
            memset(&settings, 0, sizeof(settings));
            settings.on_url = OnUrl;
            settings.on_body = OnBody;
            std::unique_ptr<http_parser> parser(new http_parser);
            char buf[kReadBufferSize];
            char response_buf[MAX_RESPONSE_SIZE];
            int nfds = 0;
            while (true) {
                nfds = epoll_wait(epollfd, events, max_events, 0);
                if (nfds == -1) {
                    if (errno == EINTR) {
                        continue;
                    }
                    perror("epoll_wait");
                    exit(EXIT_FAILURE);
                }

                for (int n = 0; n < nfds; ++n) {
                    int sock = events[n].data.fd;
                    int readed = 0;
                    if ((readed = read(sock, buf, kReadBufferSize)) <= 0) {
                        close(sock);
                        continue;
                    }
                    HttpData http_data;
                    http_data.res.response_buf = response_buf;
                    if (buf[0] == 'G') {
                        char *url_start = buf + 4;
                        http_data.url = url_start;
                        char *it = url_start;
                        int url_len = 0;
                        while (*it++ != ' ') {
                            ++url_len;
                        }
                        http_data.url_length = url_len;
                        http_data.method = HTTP_GET;
                    } else {
                        http_parser_init(parser.get(), HTTP_REQUEST);
                        parser->data = &http_data;
                        int nparsed = http_parser_execute(parser.get(), &settings, buf, readed);
                        if (nparsed != readed) {
                            close(sock);
                            continue;
                        }
                        http_data.method = parser->method;
                    }
                    Route(adapterPtr, http_data);

                    if (http_data.res.iov_size) {
                        size_t batches = http_data.res.iov_size / MAX_IOVEC_PART;
                        bool failed = false;
                        for (size_t i = 0; i < batches; ++i) {
                            if (writev(sock, &http_data.res.iov[i * MAX_IOVEC_PART],
                                       MAX_IOVEC_PART) <= 0) {
                                close(sock);
                                failed = true;
                                break;
                            }
                        }
                        if (failed) {
                            continue;
                        }
                        if (http_data.res.iov_size != MAX_IOVEC_PART) {
                            if (writev(sock, &http_data.res.iov[batches * MAX_IOVEC_PART],
                                       http_data.res.iov_size - batches * MAX_IOVEC_PART) <= 0) {
                                close(sock);
                                continue;
                            }
                        }
                    }
                    if (http_data.method == HTTP_POST) {
                        close(sock);
                        continue;
                    }
                    struct epoll_event ev;
                    ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
                    ev.data.fd = sock;
                    if (epoll_ctl(epollfd, EPOLL_CTL_MOD, sock, &ev) == -1) {
                        perror("epoll_ctl: conn_sock");
                        exit(EXIT_FAILURE);
                    }
                }
            }
        }));
    }

    int new_buf_size = 1024 * 512;
    std::cout << "listening to port " << port << "..." << std::endl;
    while (true) {
        int sock = accept(listener, NULL, NULL);
        if (sock == -1) {
            perror("accept");
            exit(EXIT_FAILURE);
        }

        setsockopt(sock, SOL_SOCKET, SO_SNDBUF, (void *)&new_buf_size, sizeof(new_buf_size));
        setsockopt(sock, SOL_SOCKET, SO_RCVBUF, (void *)&new_buf_size, sizeof(new_buf_size));
        setsockopt(sock, SOL_SOCKET, SO_DONTROUTE, (void *)&i, sizeof(i));
        setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (void *)&i, sizeof(i));
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
        ev.data.fd = sock;
        if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sock, &ev) == -1) {
            perror("epoll_ctl: conn_sock");
            exit(EXIT_FAILURE);
        }
    }

    for (auto &t : threads) {
        t.join();
    }
}

#else
void runEpollServer(Server* server, std::string port) {
    std::cout << "ERROR: Epoll is not supported on MacOS, exiting..." << std::endl;
}
#endif
