#pragma once

#include "Base.h"

#include <sys/sysctl.h>
#include <sys/types.h>

#ifdef __linux__
#include <sys/sysinfo.h>
#else
#include <mach/mach.h>
#endif

#include "stdio.h"
#include "stdlib.h"
#include "string.h"

int parseLine(char* line) {
    // This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char* p = line;
    while (*p < '0' || *p > '9') p++;
    line[i - 3] = '\0';
    i = atoi(p);
    return i;
}

inline int getUsedMemory() {  // Note: this value is in KB!
#ifdef __linux__
    FILE* file = fopen("/proc/self/status", "r");
    int64_t result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    return result * 1024;
#else
    struct task_basic_info t_info;
    mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;

    if (KERN_SUCCESS !=
        task_info(mach_task_self(), TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count)) {
        return -1;
    }
    return t_info.resident_size;
#endif
}

void printUsedMemory() {
    auto used = getUsedMemory();
    std::cout << "Used RAM: " << (double)used / (1024 * 1024) << " Mb" << std::endl;
}

double getUsedMemoryInMb() {
    auto used = getUsedMemory();
    return (double)used / (1024 * 1024);
}

inline int64_t getAvialableMemory() {
#ifdef __linux__
    struct sysinfo memInfo;
    sysinfo(&memInfo);
    long long totalAvailable = memInfo.freeram;
    // Add other values in next statement to avoid int overflow on right hand side...
    // totalVirtualMem += memInfo.totalswap;
    totalAvailable *= memInfo.mem_unit;
    return totalAvailable;
#else
    int mib[2];
    int64_t physical_memory;
    mib[0] = CTL_HW;
    mib[1] = HW_MEMSIZE;
    auto length = sizeof(int64_t);
    sysctl(mib, 2, &physical_memory, &length, NULL, 0);
    return physical_memory;
#endif
}

void printAvailableMemory() {
    auto available = getAvialableMemory();
    std::cout << "Available RAM: " << (double)available / (1024 * 1024) << " Mb" << std::endl;
}

using AllocationDataType = std::vector<std::unique_ptr<std::vector<char>>>;
AllocationDataType allocationTest() {
    std::cout << "Running allocation test" << std::endl;
    AllocationDataType pages;
    int64_t totalAllocatedMb = 0;
    for (int i = 0; i < 20; ++i) {
        int amount = 100'000'000;  // 100Mb
        totalAllocatedMb += 100;
        auto data = std::make_unique<std::vector<char>>(amount, i + 1);
        std::cout << "Allocated " << totalAllocatedMb << " Mb"
                  << " size = " << data->size() << std::endl;
        pages.push_back(std::move(data));
        printUsedMemory();
    }
    return pages;
}
