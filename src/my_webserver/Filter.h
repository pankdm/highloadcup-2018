#pragma once

#include "Base.h"
#include "Iterator.h"
#include "Types.h"
#include "Util.h"

void validateBooleanValue(const std::string& s) {
    if (s == "0" || s == "1") {
        return;
    }
    THROW_ERROR("Unexpected value for null predicate: " + s);
}

bool checkBooleanValue(bool isTrue, const std::string& boolValue) {
    if (boolValue == "1") {
        return isTrue;
    } else {
        // nullValue is 0
        // selecting non-null values
        return !isTrue;
    }
}

bool checkIfFieldIsPresent(const std::string& field, const std::string& boolValue) {
    return checkBooleanValue(field.empty(), boolValue);
}

struct Filter {
    std::string name;
    const IndexStorage* index{nullptr};

    virtual ~Filter() = default;

    static std::unique_ptr<Filter> parseSelector(const std::string& field,
                                                 const std::string& predicate,
                                                 const std::string& value,
                                                 const IndexStorage& index);
    static std::unique_ptr<Filter> parseGroupFilter(const std::string& field,
                                                    const std::string& value,
                                                    const IndexStorage& index);
    virtual bool matches(AccountId accountId, const AccountData& data) = 0;

    virtual bool supportsLookup() { return false; }

    virtual int32_t estimateOutputSize() {
        // should only be called when supportsLookup == true
        auto iterator = findRemainingItems();
        return iterator->size();
    }

    // TODO: use static
    // const AccountIdList emptyIdList;

    // Iterator is expected to go from biggest ids to smallest ones
    virtual std::unique_ptr<IdIterator> findRemainingItems() {
        // should only be called when supportsLookup == true
        MY_ASSERT(false);
    }

    // this method is only for fields that are used in group filter
    virtual int32_t getValueId() const { return INVALID_ID; }
};

struct SexFilter : public Filter {
    Sex value;

    static std::unique_ptr<Filter> parsePredicate(const std::string& predicate,
                                                  const std::string& value) {
        if (predicate != "eq") {
            THROW_ERROR("SexFilter -> Unexpected predicate: " + predicate);
        }
        validateSexValue(value);
        auto result = std::make_unique<SexFilter>();
        result->value = value;
        return result;
    }

    bool matches(AccountId accountId, const AccountData& data) override {
        return data.sex == value;
    }

    bool supportsLookup() override { return true; }

    std::unique_ptr<IdIterator> findRemainingItems() override {
        RETURN_ITERATOR_FROM_MAP(usersAtSex, value);
    }

    int32_t getValueId() const override { return static_cast<int32_t>(convertStringToSex(value)); }
};

struct EmailFilter : public Filter {
    enum class Predicate {
        LT = 0,
        GT = 1,
        DOMAIN_VALUE = 2,
    };

    Predicate predicate;
    std::string value;

    static std::unique_ptr<Filter> parsePredicate(const std::string& predicate,
                                                  const std::string& value) {
        auto result = std::make_unique<EmailFilter>();
        if (predicate == "lt") {
            result->predicate = Predicate::LT;
            result->value = value;
        } else if (predicate == "gt") {
            result->predicate = Predicate::GT;
            result->value = value;
        } else if (predicate == "domain") {
            result->predicate = Predicate::DOMAIN_VALUE;
            result->value = value;
        } else {
            THROW_ERROR("EmailFilter -> Unexpected predicate: " + predicate);
        }
        return result;
    }

    bool supportsLookup() override {
        if (predicate == Predicate::DOMAIN_VALUE) {
            return true;
        }
        return false;
    }

    std::unique_ptr<IdIterator> findRemainingItems() override {
        RETURN_ITERATOR_FROM_MAP(usersAtEmailDomain, value);
    }

    bool matches(AccountId accountId, const AccountData& data) override {
        if (predicate == Predicate::LT) {
            return data.email < value;
        } else if (predicate == Predicate::GT) {
            return data.email > value;
        } else if (predicate == Predicate::DOMAIN_VALUE) {
            return data.emailDomain == value;
        } else {
            MY_ASSERT(false);
        }
    }
};

struct StatusFilter : public Filter {
    enum class Predicate {
        EQ = 0,
        NEQ = 1,
    };

    Predicate predicate;
    Status value;

    static std::unique_ptr<Filter> parsePredicate(const std::string& predicate,
                                                  const std::string& value) {
        auto result = std::make_unique<StatusFilter>();
        result->value = convertStringToStatus(value);
        if (predicate == "eq") {
            result->predicate = Predicate::EQ;
        } else if (predicate == "neq") {
            result->predicate = Predicate::NEQ;
        } else {
            THROW_ERROR("StatusFilter -> Unexpected predicate: " + predicate);
        }
        return result;
    }

    bool matches(AccountId accountId, const AccountData& data) override {
        if (predicate == Predicate::EQ) {
            return data.status == value;
        } else if (predicate == Predicate::NEQ) {
            return data.status != value;
        } else {
            MY_ASSERT(false);
        }
    }

    bool supportsLookup() override {
        if (predicate == Predicate::EQ) {
            return true;
        }
        return false;
    }

    std::unique_ptr<IdIterator> findRemainingItems() override {
        MY_ASSERT(index);
        return std::make_unique<IdListIterator>(index->usersAtStatus[static_cast<int8_t>(value)]);
    }

    int32_t getValueId() const override { return static_cast<int32_t>(value); }
};

struct FNameFilter : public Filter {
    enum class Predicate {
        EQ = 0,
        ANY = 1,
        NULL_VALUE = 2,
    };

    Predicate predicate;
    std::string value;
    std::unordered_set<std::string> values;

    static std::unique_ptr<Filter> parsePredicate(const std::string& predicate,
                                                  const std::string& value) {
        auto result = std::make_unique<FNameFilter>();
        if (predicate == "eq") {
            result->predicate = Predicate::EQ;
            result->value = value;
        } else if (predicate == "any") {
            result->predicate = Predicate::ANY;
            auto names = splitString(value, ',');
            result->values.insert(names.begin(), names.end());
        } else if (predicate == "null") {
            result->predicate = Predicate::NULL_VALUE;
            validateBooleanValue(value);
            result->value = value;
        } else {
            THROW_ERROR("FNameFilter -> Unexpected predicate: " + predicate);
        }
        return result;
    }

    bool matches(AccountId accountId, const AccountData& data) override {
        if (predicate == Predicate::EQ) {
            return data.fname == value;
        } else if (predicate == Predicate::ANY) {
            return stl::contains(values, data.fname);
        } else if (predicate == Predicate::NULL_VALUE) {
            return checkIfFieldIsPresent(data.fname, value);
        } else {
            MY_ASSERT(false);
        }
    }
};

struct SNameFilter : public Filter {
    enum class Predicate {
        EQ = 0,
        STARTS = 1,
        NULL_VALUE = 2,
    };

    Predicate predicate;
    std::string value;

    static std::unique_ptr<Filter> parsePredicate(const std::string& predicate,
                                                  const std::string& value) {
        auto result = std::make_unique<SNameFilter>();
        result->value = value;
        if (predicate == "eq") {
            result->predicate = Predicate::EQ;
        } else if (predicate == "starts") {
            result->predicate = Predicate::STARTS;
        } else if (predicate == "null") {
            result->predicate = Predicate::NULL_VALUE;
            validateBooleanValue(value);
        } else {
            THROW_ERROR("SNameFilter -> Unexpected predicate: " + predicate);
        }
        return result;
    }

    bool matches(AccountId accountId, const AccountData& data) override {
        if (predicate == Predicate::EQ) {
            return data.sname == value;
        } else if (predicate == Predicate::STARTS) {
            return startsWith(data.sname, value);
        } else if (predicate == Predicate::NULL_VALUE) {
            return checkIfFieldIsPresent(data.sname, value);
        } else {
            MY_ASSERT(false);
        }
    }
};

struct PhoneFilter : public Filter {
    enum class Predicate {
        CODE = 0,
        NULL_VALUE = 1,
    };

    Predicate predicate;
    std::string value;

    static std::unique_ptr<Filter> parsePredicate(const std::string& predicate,
                                                  const std::string& value) {
        auto result = std::make_unique<PhoneFilter>();
        result->value = value;
        if (predicate == "code") {
            result->predicate = Predicate::CODE;
        } else if (predicate == "null") {
            result->predicate = Predicate::NULL_VALUE;
            validateBooleanValue(value);
        } else {
            THROW_ERROR("PhoneFilter -> Unexpected predicate: " + predicate);
        }
        return result;
    }

    bool matches(AccountId accountId, const AccountData& data) override {
        if (predicate == Predicate::CODE) {
            return getPhoneCode(data.phone) == value;
        } else if (predicate == Predicate::NULL_VALUE) {
            return checkIfFieldIsPresent(data.phone, value);
        } else {
            MY_ASSERT(false);
        }
    }
};

struct CountryFilter : public Filter {
    enum class Predicate {
        EQ = 0,
        NULL_VALUE = 1,
    };

    Predicate predicate;
    std::string value;

    CountryId countryId{INVALID_COUNTRY_ID};
    CountryId emptyCountryId{INVALID_COUNTRY_ID};

    static std::unique_ptr<Filter> parsePredicate(const std::string& predicate,
                                                  const std::string& value,
                                                  const IndexStorage& index) {
        auto result = std::make_unique<CountryFilter>();
        if (predicate == "eq") {
            result->predicate = Predicate::EQ;
            result->countryId = index.countryIdMap.getId(value);
            result->value = value;
        } else if (predicate == "null") {
            result->predicate = Predicate::NULL_VALUE;
            validateBooleanValue(value);
            result->value = value;
        } else {
            THROW_ERROR("CountryFilter -> Unexpected predicate: " + predicate);
        }
        return result;
    }

    bool matches(AccountId accountId, const AccountData& data) override {
        if (predicate == Predicate::EQ) {
            return data.countryId == countryId;
        } else if (predicate == Predicate::NULL_VALUE) {
            return checkIfFieldIsPresent(data.country, value);
        } else {
            MY_ASSERT(false);
        }
    }

    bool supportsLookup() override {
        if (predicate == Predicate::EQ) {
            return true;
        }
        if (predicate == Predicate::NULL_VALUE) {
            if (value == "1") {
                return true;
            }
        }
        return false;
    }

    std::unique_ptr<IdIterator> findRemainingItems() override {
        MY_ASSERT(index);
        // keep it consistent with supportsLookup
        if (predicate == Predicate::EQ) {
            RETURN_ITERATOR_FROM_MAP(usersAtCountry, value)
        }
        if (predicate == Predicate::NULL_VALUE) {
            if (value == "1") {
                RETURN_ITERATOR_FROM_MAP(usersAtCountry, "");
            }
        }
        MY_ASSERT(false);
    }

    int32_t getValueId() const override { return countryId; }
};

struct CityFilter : public Filter {
    enum class Predicate {
        EQ = 0,
        ANY = 1,
        NULL_VALUE = 2,
    };

    Predicate predicate;
    std::string value;

    CityId cityId;
    std::unordered_set<CityId> cityIds;

    static std::unique_ptr<Filter> parsePredicate(const std::string& predicate,
                                                  const std::string& value,
                                                  const IndexStorage& index) {
        auto result = std::make_unique<CityFilter>();
        if (predicate == "eq") {
            result->predicate = Predicate::EQ;
            result->cityId = index.cityIdMap.getId(value);
            result->value = value;
        } else if (predicate == "any") {
            result->predicate = Predicate::ANY;
            auto cities = splitString(value, ',');
            for (const auto& city : cities) {
                result->cityIds.insert(index.cityIdMap.getId(city));
            }
        } else if (predicate == "null") {
            result->predicate = Predicate::NULL_VALUE;
            validateBooleanValue(value);
            result->value = value;
        } else {
            THROW_ERROR("CityFilter -> Unexpected predicate: " + predicate);
        }
        return result;
    }

    bool matches(AccountId accountId, const AccountData& data) override {
        if (predicate == Predicate::EQ) {
            return data.cityId == cityId;
        } else if (predicate == Predicate::ANY) {
            return stl::contains(cityIds, data.cityId);
        } else if (predicate == Predicate::NULL_VALUE) {
            return checkIfFieldIsPresent(data.city, value);
        } else {
            MY_ASSERT(false);
        }
    }

    bool supportsLookup() override {
        if (predicate == Predicate::EQ) {
            return true;
        }
        if (predicate == Predicate::NULL_VALUE) {
            if (value == "1") {
                return true;
            }
        }
        return false;
    }

    std::unique_ptr<IdIterator> findRemainingItems() override {
        MY_ASSERT(index);
        // keep it consistent with supportsLookup
        if (predicate == Predicate::EQ) {
            RETURN_ITERATOR_FROM_MAP(usersAtCity, value)
        }
        if (predicate == Predicate::NULL_VALUE) {
            if (value == "1") {
                RETURN_ITERATOR_FROM_MAP(usersAtCity, "");
            }
        }
        MY_ASSERT(false);
    }

    int32_t getValueId() const override { return cityId; }
};

struct BirthFilter : public Filter {
    enum class Predicate {
        LT = 0,
        GT = 1,
        YEAR = 2,
    };

    Predicate predicate;
    Timestamp value;
    YearShort year;

    static std::unique_ptr<Filter> parsePredicate(const std::string& predicate,
                                                  const std::string& value) {
        auto result = std::make_unique<BirthFilter>();
        if (predicate == "lt") {
            result->predicate = Predicate::LT;
            result->value = parseAsTimestamp(value);
        } else if (predicate == "gt") {
            result->predicate = Predicate::GT;
            result->value = parseAsTimestamp(value);
        } else if (predicate == "year") {
            result->predicate = Predicate::YEAR;
            result->year = (parseAsInt32(value) - BASE_YEAR);
        } else {
            THROW_ERROR("BirthFilter -> Unexpected predicate: " + predicate);
        }
        return result;
    }

    bool matches(AccountId accountId, const AccountData& data) override {
        if (predicate == Predicate::LT) {
            return data.birth < value;
        } else if (predicate == Predicate::GT) {
            return data.birth > value;
        } else if (predicate == Predicate::YEAR) {
            return data.birthYear == year;
        } else {
            MY_ASSERT(false);
        }
    }

    bool supportsLookup() override {
        if (predicate == Predicate::YEAR) {
            return true;
        }
        return false;
    }

    std::unique_ptr<IdIterator> findRemainingItems() override {
        RETURN_ITERATOR_FROM_MAP(usersAtBirthYear, year);
    }

    int32_t getValueId() const override { return year; }
};

struct JoinedFilter : public Filter {
    enum class Predicate {
        YEAR = 0,
    };

    Predicate predicate;
    YearShort year;

    static std::unique_ptr<Filter> parsePredicate(const std::string& predicate,
                                                  const std::string& value) {
        auto result = std::make_unique<JoinedFilter>();
        if (predicate == "year") {
            result->predicate = Predicate::YEAR;
            result->year = (parseAsInt32(value) - BASE_YEAR);
        } else {
            THROW_ERROR("JoinedFilter -> Unexpected predicate: " + predicate);
        }
        return result;
    }

    bool matches(AccountId accountId, const AccountData& data) override {
        return data.joinedYear == year;
    }

    bool supportsLookup() override { return true; }

    std::unique_ptr<IdIterator> findRemainingItems() override {
        RETURN_ITERATOR_FROM_MAP(usersAtJoinedYear, year);
    }

    int32_t getValueId() const override { return year; }
};

struct InterestsFilter : public Filter {
    enum class Predicate {
        CONTAINS = 0,
        ANY = 1,
    };

    Predicate predicate;
    std::unordered_set<InterestId> values;
    std::vector<InterestId> valuesVec;

    static std::unique_ptr<Filter> parsePredicate(const std::string& predicate,
                                                  const std::string& value,
                                                  const IndexStorage& index) {
        auto result = std::make_unique<InterestsFilter>();
        auto interests = splitString(value, ',');
        for (const auto& interest : interests) {
            auto interestId = index.interestIdMap.getId(interest);
            result->values.insert(interestId);
            result->valuesVec.push_back(interestId);
        }
        SORT_REVERSE(result->valuesVec);

        if (predicate == "contains") {
            result->predicate = Predicate::CONTAINS;
        } else if (predicate == "any") {
            result->predicate = Predicate::ANY;
        } else {
            THROW_ERROR("InterestsFilter -> Unexpected predicate: " + predicate);
        }
        return result;
    }

    bool matches(AccountId accountId, const AccountData& data) override {
        // 1 values optimization
        if (valuesVec.size() == 1) {
            auto needle = valuesVec[0];
            for (const auto interestId : data.interests) {
                if (needle == interestId) {
                    return true;
                }
            }
            return false;
        }

        if (predicate == Predicate::CONTAINS) {
            int idx1 = 0;
            int idx2 = 0;
            while (true) {
                if (idx1 >= valuesVec.size()) {
                    return true;
                }
                if (idx2 >= data.interests.size()) {
                    return false;
                }
                auto left = valuesVec[idx1];
                auto right = data.interests[idx2];
                if (left == right) {
                    ++idx1;
                    ++idx2;
                    continue;
                }
                if (left > right) {
                    return false;
                } else {
                    ++idx2;
                    continue;
                }
            }
            return true;
        } else if (predicate == Predicate::ANY) {
            const auto& a = valuesVec;
            const auto& b = data.interests;
            int ia = 0;
            int ib = 0;
            while (true) {
                if (ia >= a.size() || ib >= b.size()) {
                    return false;
                }
                auto va = a[ia];
                auto vb = b[ib];
                if (va == vb) {
                    return true;
                }
                if (va > vb) {
                    ++ia;
                    continue;
                } else {
                    ++ib;
                    continue;
                }
            }
            // for (const auto& interest : data.interests) {
            //     if (stl::contains(values, interest)) {
            //         return true;
            //     }
            // }
            // return false;
        } else {
            MY_ASSERT(false);
        }
    }

    bool supportsLookup() override {
        if (predicate == Predicate::CONTAINS) {
            return true;
        }
        if (predicate == Predicate::ANY) {
            if (values.size() == 1) {
                return true;
            }
        }
        return false;
    }

    std::unique_ptr<IdIterator> findRemainingItems() override {
        if (predicate == Predicate::CONTAINS) {
            MY_ASSERT(index);
            InterestId bestInterestId = INVALID_INTEREST_ID;
            int32_t bestSize = 0;
            for (const auto& interestId : values) {
                int32_t currentSize = index->usersAtInterestId[interestId].size();
                if (bestInterestId == INVALID_INTEREST_ID || currentSize < bestSize) {
                    bestSize = currentSize;
                    bestInterestId = interestId;
                }
            }
            return std::make_unique<IdListIterator>(index->usersAtInterestId[bestInterestId]);
        } else {
            MY_ASSERT(valuesVec.size() == 1);
            auto interestId = valuesVec[0];
            if (interestId == INVALID_INTEREST_ID) {
                return CREATE_EMPTY_ITERATOR();
            } else {
                return std::make_unique<IdListIterator>(index->usersAtInterestId[interestId]);
            }
        }
    }

    int32_t getValueId() const override {
        MY_ASSERT_EQ(values.size(), 1);
        return *values.begin();
    }
};

struct LikesFilter : public Filter {
    // only 1 predicate is supported: containes
    // "who liked all given users"

    std::vector<AccountId> values;
    static std::unique_ptr<Filter> parsePredicate(const std::string& predicate,
                                                  const std::string& value) {
        auto result = std::make_unique<LikesFilter>();
        auto parts = splitString(value, ',');
        result->values.reserve(parts.size());
        for (const auto& idStr : parts) {
            result->values.push_back(parseAsAccountId(idStr));
        }
        std::sort(result->values.rbegin(), result->values.rend());

        if (predicate != "contains") {
            THROW_ERROR("LikesFilter -> Unexpected predicate: " + predicate);
        }
        return result;
    }

    bool matches(AccountId accountId, const AccountData& data) override {
        // std::cout << "trying to match with account " << accountId << std::endl;
        int idx1 = 0;
        int idx2 = 0;
        const auto& likes = data.likes;
        while (true) {
            if (idx1 >= values.size()) {
                return true;
            }
            if (idx2 >= likes.size()) {
                return false;
            }
            auto left = values[idx1];
            auto right = likes[idx2].accountId;
            // std::cout << " idx1 = " << idx1 << " idx2 = " << idx2 << std::endl;
            // std::cout << " comparing " << left << " vs " << right << std::endl;
            if (left == right) {
                ++idx1;
                ++idx2;
                continue;
            }
            if (left > right) {
                return false;
            } else {
                ++idx2;
                continue;
            }
        }
        // for (const auto& id : values) {
        //     bool found = false;
        //     for (const auto& edge : data.likes) {
        //         if (edge.accountId == id) {
        //             found = true;
        //             break;
        //         }
        //     }
        //     if (!found) {
        //         return false;
        //     }
        // }
        return true;
    }

    bool supportsLookup() override {
        // NOTE: fix rewrite query if increasing number of values supported
        if (values.size() <= 3) {
            return true;
        }
        return false;
    }
    std::unique_ptr<IdIterator> createSingleValueIterator(AccountId id) {
        if (isValidId(id) && isValidId(index->accountsArray[id].id)) {
            return std::make_unique<LikeEdgeIterator>(index->accountsArray[id].backwardLikes);
        } else {
            return CREATE_EMPTY_ITERATOR();
        }
    }

    std::unique_ptr<IdIterator> findRemainingItems() override {
        MY_ASSERT(index);
        if (values.size() == 1) {
            return createSingleValueIterator(values[0]);
        } else if (values.size() == 2) {
            auto a = createSingleValueIterator(values[0]);
            auto b = createSingleValueIterator(values[1]);
            return std::make_unique<IntersectionIdIterator>(std::move(a), std::move(b));
        } else {
            MY_ASSERT(values.size() == 3);
            auto a = createSingleValueIterator(values[0]);
            auto b = createSingleValueIterator(values[1]);
            auto c = createSingleValueIterator(values[2]);
            auto it1 = std::make_unique<IntersectionIdIterator>(std::move(a), std::move(b));
            return std::make_unique<IntersectionIdIterator>(std::move(it1), std::move(c));
        }
    }
};

struct PremiumFilter : public Filter {
    enum class Predicate {
        NOW = 0,
        NULL_VALUE = 1,
    };

    Predicate predicate;
    std::string value;

    static std::unique_ptr<Filter> parsePredicate(const std::string& predicate,
                                                  const std::string& value) {
        auto result = std::make_unique<PremiumFilter>();
        if (predicate == "now") {
            result->predicate = Predicate::NOW;
            validateBooleanValue(value);
            result->value = value;
        } else if (predicate == "null") {
            result->predicate = Predicate::NULL_VALUE;
            validateBooleanValue(value);
            result->value = value;
        } else {
            THROW_ERROR("PremiumFilter -> Unexpected predicate: " + predicate);
        }
        return result;
    }

    bool matches(AccountId accountId, const AccountData& data) override {
        if (predicate == Predicate::NOW) {
            return checkBooleanValue(data.hasPremiumNow, value);
        } else if (predicate == Predicate::NULL_VALUE) {
            return checkBooleanValue(data.premiumStart == 0, value);
        } else {
            MY_ASSERT(false);
        }
    }
};

std::unique_ptr<Filter> Filter::parseSelector(const std::string& field,
                                              const std::string& predicate,
                                              const std::string& value, const IndexStorage& index) {
    std::unique_ptr<Filter> result;
    if (field == "sex") {
        result = SexFilter::parsePredicate(predicate, value);
    } else if (field == "email") {
        result = EmailFilter::parsePredicate(predicate, value);
    } else if (field == "status") {
        result = StatusFilter::parsePredicate(predicate, value);
    } else if (field == "fname") {
        result = FNameFilter::parsePredicate(predicate, value);
    } else if (field == "sname") {
        result = SNameFilter::parsePredicate(predicate, value);
    } else if (field == "phone") {
        result = PhoneFilter::parsePredicate(predicate, value);
    } else if (field == "country") {
        result = CountryFilter::parsePredicate(predicate, value, index);
    } else if (field == "city") {
        result = CityFilter::parsePredicate(predicate, value, index);
    } else if (field == "birth") {
        result = BirthFilter::parsePredicate(predicate, value);
    } else if (field == "interests") {
        result = InterestsFilter::parsePredicate(predicate, value, index);
    } else if (field == "likes") {
        result = LikesFilter::parsePredicate(predicate, value);
    } else if (field == "premium") {
        result = PremiumFilter::parsePredicate(predicate, value);
    } else {
        THROW_ERROR("Unexpected field: " + field);
    }
    result->name = field;
    result->index = &index;
    return result;
}

std::unique_ptr<Filter> Filter::parseGroupFilter(const std::string& field, const std::string& value,
                                                 const IndexStorage& index) {
    std::unique_ptr<Filter> result;
    if (field == "sex") {
        result = SexFilter::parsePredicate("eq", value);
    } else if (field == "email") {
        throw UnsupportedException("Filter by email in group API is unsupported");
    } else if (field == "status") {
        result = StatusFilter::parsePredicate("eq", value);
    } else if (field == "fname") {
        result = FNameFilter::parsePredicate("eq", value);
    } else if (field == "sname") {
        result = SNameFilter::parsePredicate("eq", value);
    } else if (field == "phone") {
        throw UnsupportedException("Filter by phone in group API is unsupported");
    } else if (field == "country") {
        result = CountryFilter::parsePredicate("eq", value, index);
    } else if (field == "city") {
        result = CityFilter::parsePredicate("eq", value, index);
    } else if (field == "birth") {
        result = BirthFilter::parsePredicate("year", value);
    } else if (field == "joined") {
        result = JoinedFilter::parsePredicate("year", value);
    } else if (field == "interests") {
        result = InterestsFilter::parsePredicate("contains", value, index);
    } else if (field == "likes") {
        result = LikesFilter::parsePredicate("contains", value);
    } else if (field == "premium") {
        throw UnsupportedException("Filter by premium in group API is unsupported");
    } else {
        THROW_ERROR("Unexpected field: " + field);
    }
    result->name = field;
    result->index = &index;
    return result;
}

class FilterList {
   public:
    static std::unique_ptr<FilterList> parse(const RequestParams& params,
                                             const IndexStorage& index) {
        auto result = std::make_unique<FilterList>();
        result->filters.reserve(params.size());
        for (const auto& [key, value] : params) {
            if (key == "query_id" || key == "limit") {
                // skip known fields
                continue;
            }
            auto parts = splitString(key, '_');
            if (parts.size() != 2) {
                THROW_ERROR("Wrong format of key: " + key + " expected field_predicate");
            }
            auto field = parts[0];
            auto predicate = parts[1];

            auto filter = Filter::parseSelector(field, predicate, value, index);

            result->filters.push_back(std::move(filter));
            result->selectedFields.insert(field);
        }
        return std::move(result);
    }

    bool matches(AccountId accountId, const AccountData& data) {
        for (const auto& filter : filters) {
            if (!filter->matches(accountId, data)) {
                return false;
            }
        }
        return true;
    }

    std::vector<std::unique_ptr<Filter>> filters;
    SelectedFields selectedFields;
    int32_t limit{-1};
};

struct LocationFilter {
    // returns nullptr if no filters selected
    static std::unique_ptr<Filter> parse(const RequestParams& params, const IndexStorage& index) {
        std::string country;
        std::string city;
        for (const auto& [key, value] : params) {
            if (key == "query_id" || key == "limit") {
                continue;
            }
            if (key == "country") {
                if (value.empty()) {
                    THROW_ERROR("LocationFilter -> Empty value of param country");
                }
                country = value;
            } else if (key == "city") {
                if (value.empty()) {
                    THROW_ERROR("LocationFilter -> Empty value of param city");
                }
                city = value;
            } else {
                THROW_ERROR("LocationFilter -> Unexpected parameter: " + key);
            }
        }
        if (!country.empty() && !city.empty()) {
            THROW_ERROR("LocationFilter ->  Both city and country filters are present");
        }
        if (!country.empty()) {
            return CountryFilter::parsePredicate("eq", country, index);
        }
        if (!city.empty()) {
            return CityFilter::parsePredicate("eq", city, index);
        }
        return nullptr;
    }
};

struct OptimizedFilter {
    bool matches(AccountId accountId, const AccountData& data) {
        for (const auto& filter : filters) {
            if (!filter->matches(accountId, data)) {
                return false;
            }
        }
        return true;
    }

    std::unique_ptr<Filter> lookupFilter;
    std::vector<std::unique_ptr<Filter>> filters;
};
