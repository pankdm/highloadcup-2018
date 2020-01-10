#include "Base.h"
#include "EpollServer.h"
#include "HttpHandler.h"
#include "MemoryUsage.h"
#include "Server.h"
#include "Tests.h"
#include "Timer.h"
#include "Util.h"

static const std::string DOCUMENT_ROOT = ".";

#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define PRINT_SIZEOF(C) std::cout << "sizeof " #C << " = " << sizeof(C) << std::endl;
#define PRINT_CONFIG(C) std::cout << "config " #C << " = " << C << std::endl;

void handler(int sig) {
    void* array[10];
    size_t size;

    // get void*'s for all entries on the stack
    size = backtrace(array, 10);

    // print out all the frames to stderr
    fprintf(stderr, "Error: signal %d:\n", sig);
    backtrace_symbols_fd(array, size, STDERR_FILENO);
    exit(1);
}

int main(int argc, char* argv[]) {
    std::srand(std::time(nullptr));  // use current time as seed for random generator
    signal(SIGSEGV, handler);
    signal(SIGABRT, handler);

    GLOBAL_TIMER.start();

    std::cout << "Running tests..." << std::endl;
    runTests();

    if (argc == 1) {
        std::cout << "OK" << std::endl;
        return 0;
    }

    if (argc < 3) {
        std::cout << "usage: <BINARY> port data_directory" << std::endl;
        return 1;
    }
    printAvailableMemory();
    printUsedMemory();

    // auto data = allocationTest();

    std::string port(argv[1]);
    std::string dir(argv[2]);

    // PRINT_SIZEOF(size_t);
    // PRINT_SIZEOF(InterestId);
    // PRINT_SIZEOF(LikeEdge);
    PRINT_SIZEOF(Status);
    PRINT_SIZEOF(AccountData);

    readPremiumNow(dir + "/options.txt");
    PRINT_CONFIG(PREMIUM_NOW);
    PRINT_CONFIG(MAX_ACCOUNT_ID);
    PRINT_CONFIG(LOGGING_LEVEL);
    PRINT_CONFIG(NUM_SUPPORTED_BREAKDOWNS);
    PRINT_CONFIG(NUM_CONCURRENT_REQUESTS);
    PRINT_CONFIG(ENABLE_RECOMMEND_API);
    PRINT_CONFIG(RANDOM_RECOMMEND_RATE);

    Server server;
    Timer t;
    t.start();
    server.loadDataFromDirectory(dir);
    t.stop();
    std::cout << "Finished loading in " << (double)t.elapsedMilliseconds() / 1000. << " s"
              << std::endl;

    if (USE_FAST_HTTP_SERVER) {
        std::cout << "Using FAST http server" << std::endl;
        runEpollServer(&server, port);
    } else {
        std::cout << "Using CIVETSERVER http server" << std::endl;
        std::vector<std::string> cppOptions = {
            "document_root",      DOCUMENT_ROOT,  // root
            "listening_ports",    port,           // port
            "request_timeout_ms", "2200",         // 2.2 seconds
            "num_threads",        "50",           // 4 is number of threads given
            // "enable_keep_alive",     "yes",          // should improve perf
            // "keep_alive_timeout_ms", "500",          // 500ms
        };
        CivetServer civetServer(cppOptions);  // <-- C++ style start

        HttpHandler handler(&server);
        civetServer.addHandler("", handler);

        std::cout << "listening to port " << port << " ..." << std::endl;
        printUsedMemory();

        while (true) {
            sleep(10);
        }
    }
    // std::cout << "data size = " << data.size() << std::endl;

    printf("Bye!\n");
    return 0;
}
