#pragma once

#include "Iterator.h"
#include "Types.h"

namespace tests {
void runIteratorTests() {
    std::cout << "Running Iterator tests " << std::endl;
    AccountIdList a = {10, 10, 5, 3, 2};
    AccountIdList b = {15, 10, 6, 2, 1};

    auto aIt = std::make_unique<IdListIterator>(a);
    auto bIt = std::make_unique<IdListIterator>(b);

    auto intersection = std::make_unique<IntersectionIdIterator>(std::move(aIt), std::move(bIt));

    AccountIdList c;
    AccountIdList cExpected = {10, 2};
    for (; intersection->valid(); intersection->next()) {
        c.push_back(intersection->getId());
    }
    for (int i = 0; i < c.size(); ++i) {
        MY_ASSERT_EQ(c[i], cExpected[i]);
    }
}
}  // namespace tests

void runTests() {
    MY_ASSERT_EQ(getYearFromTimestamp(893884157), 1998);
    MY_ASSERT_EQ(getPhoneCode("8(974)1210264"), "974");
    MY_ASSERT_EQ(getPhoneCode("8()1210264"), "");
    MY_ASSERT_EQ(convertSexToString(getOppositeSexEnum(SexEnum::MALE)), "f");
    MY_ASSERT_EQ(convertSexToString(getOppositeSexEnum(SexEnum::FEMALE)), "m");

    tests::runIteratorTests();
}
