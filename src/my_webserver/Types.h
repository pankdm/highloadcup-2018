#pragma once

#include "Base.h"
#include "Globals.h"
#include "IdValueMap.h"
#include "Util.h"

using SelectedFields = std::unordered_set<std::string>;

using RequestParams = std::unordered_map<std::string, std::string>;

int32_t parseAsInt32(const std::string& s) {
    if (!isDigit(s)) {
        THROW_ERROR("Invalid int32_t format: " + s);
    }
    return convert<int32_t>(s);
}

using Timestamp = int32_t;
Timestamp parseAsTimestamp(const std::string& s) {
    // TODO: use is_same trait
    static_assert(sizeof(Timestamp) == sizeof(int32_t));
    return parseAsInt32(s);
}

// number of years starting from 1900
using YearShort = int8_t;

int32_t getYearFromTimestamp(Timestamp time) {
    time_t rawtime = time;
    auto ptm = gmtime(&rawtime);
    return 1900 + ptm->tm_year;
}

constexpr int BASE_YEAR = 1900;

using AccountId = int32_t;
// constexpr AccountId MAX_ACCOUNT_ID = 1300000;
constexpr AccountId MAX_ACCOUNT_ID = 1320000;

constexpr double LIKES_GROWTH_COEFFICIENT = 1.1;

double getGoodLikesSize(int32_t currentSize) {
    return int(LIKES_GROWTH_COEFFICIENT * currentSize + 1);
}

// constexpr AccountId MAX_ACCOUNT_ID = 300000;
constexpr AccountId EMPTY_ACCOUNT_ID = 0;

// MACROS:
#define FOR_EACH_ACCOUNT_ID(id) for (int id = 1; id <= MAX_ACCOUNT_ID; ++id)

// only to be called in a loop
#define TRY_GET_CONST_DATA(id, data)            \
    const auto& data = index.accountsArray[id]; \
    if (!isValidId(data.id)) {                  \
        continue;                               \
    }

#define TRY_GET_NONCONST_DATA(id, data)   \
    auto& data = index.accountsArray[id]; \
    if (!isValidId(data.id)) {            \
        continue;                         \
    }

AccountId parseAsAccountId(const std::string& s) {
    static_assert(sizeof(AccountId) == sizeof(int32_t));
    return parseAsInt32(s);
}

inline bool isValidId(AccountId id) { return (0 < id && id <= MAX_ACCOUNT_ID); }

int32_t extractLimitParam(const RequestParams& params) {
    auto limitPtr = stl::mapGetPtr(params, "limit");
    if (!limitPtr || !isDigit(*limitPtr)) {
        THROW_ERROR("Missing or incorrect limit param");
    }
    int32_t limit = convert<int32_t>(*limitPtr);
    if (limit <= 0) {
        THROW_ERROR("Limit param should be positive, got: " + (*limitPtr));
    }
    return limit;
}

using InterestId = int8_t;
constexpr InterestId INVALID_INTEREST_ID = -1;

using CountryId = int8_t;
constexpr CountryId INVALID_COUNTRY_ID = -1;

using CityId = int16_t;
constexpr CityId INVALID_CITY_ID = -1;

constexpr int32_t INVALID_ID = -1;

enum class Status : int8_t {
    SINGLE = 0,
    COMPLICATED = 1,
    IN_RELATIONSHIPS = 2,
};

constexpr int32_t STATUS_CNT = 3;
constexpr int32_t PREMIUM_CNT = 2;
constexpr int32_t SEX_CNT = 2;

constexpr char SINGLE_STR[] = "свободны";
constexpr char COMPLICATED_STR[] = "всё сложно";
constexpr char IN_RELATIONSHIPS_STR[] = "заняты";

std::string convertStatusToString(Status status) {
    if (status == Status::SINGLE) {
        return SINGLE_STR;
    } else if (status == Status::COMPLICATED) {
        return COMPLICATED_STR;
    } else if (status == Status::IN_RELATIONSHIPS) {
        return IN_RELATIONSHIPS_STR;
    } else {
        MY_ASSERT(false);  // unknown status
    }
}

Status convertStringToStatus(const std::string& s) {
    if (s == SINGLE_STR) {
        return Status::SINGLE;
    } else if (s == COMPLICATED_STR) {
        return Status::COMPLICATED;
    } else if (s == IN_RELATIONSHIPS_STR) {
        return Status::IN_RELATIONSHIPS;
    } else {
        THROW_ERROR("Unknown status string: " + s);
    }
}

using Sex = std::string;

enum class SexEnum : int8_t {
    MALE = 0,
    FEMALE = 1,
};

std::string convertSexToString(SexEnum sex) {
    if (sex == SexEnum::MALE) {
        return "m";
    } else if (sex == SexEnum::FEMALE) {
        return "f";
    } else {
        MY_ASSERT(false);
    }
}

SexEnum convertStringToSex(const std::string& s) {
    if (s == "m") {
        return SexEnum::MALE;
    } else if (s == "f") {
        return SexEnum::FEMALE;
    } else {
        THROW_ERROR("Unexpected Sex value: " + s);
    }
}

void validateSexValue(const Sex& s) {
    if (s == "m" || s == "f") {
        return;
    }
    THROW_ERROR("Unexpected Sex value: " + s);
}

std::string getOppositeSex(const Sex& s) {
    if (s == "m") return "f";
    if (s == "f") return "m";
    THROW_ERROR("Unexpected Sex value: " + s);
}

SexEnum getOppositeSexEnum(SexEnum s) {
    if (s == SexEnum::MALE)
        return SexEnum::FEMALE;
    else
        return SexEnum::MALE;
}

std::string getEmailDomain(const std::string& email) {
    auto parts = splitString(email, '@');
    if (parts.size() != 2) {
        THROW_ERROR("Invalid email format: " + email);
    }
    return parts[1];
}

std::string getPhoneCode(const std::string& phone) {
    auto start = phone.find("(");
    if (start == std::string::npos) {
        return "";
    }
    auto end = phone.find(")", start);
    if (end == std::string::npos) {
        return "";
    }
    return phone.substr(start + 1, end - start - 1);
}

struct LikeInput {
    AccountId liker;
    AccountId likee;
    Timestamp ts;
};

struct LikeEdge {
    AccountId accountId{INVALID_ID};
    Timestamp ts{0};
    LikeEdge() = default;

    LikeEdge(AccountId accountId_, Timestamp ts_) : accountId(accountId_), ts(ts_) {}
};

using EdgeList = std::vector<LikeEdge>;

struct AccountData {
    AccountId id{EMPTY_ACCOUNT_ID};

    std::string fname;
    std::string sname;
    std::string email;
    std::string phone;
    // std::string status;

    std::string country;
    std::string city;

    std::string sex;

    Timestamp birth{0};

    Timestamp joined{0};
    Timestamp premiumStart{0};
    Timestamp premiumFinish{0};

    // using EdgeList = std::unordered_map<AccountId, LikeEdge>;
    EdgeList likes;
    EdgeList backwardLikes;

    std::vector<InterestId> interests;

    bool hasPremiumNow{false};
    Status status{Status::SINGLE};
    YearShort joinedYear{0};
    YearShort birthYear{0};

    CountryId countryId{INVALID_COUNTRY_ID};
    CityId cityId{INVALID_CITY_ID};
    SexEnum sexEnum{SexEnum::MALE};
    std::string emailDomain;

    void toJson(json& j) const {
        // toJson(j["likes"], likes);
        // toJson(j["backwardLikes"], backwardLikes);
        //
        j["fname"] = fname;
        j["sname"] = sname;
        j["email"] = email;
        j["country"] = country;
        j["city"] = city;
        j["interests"] = json::array();
        auto interestsTmp = interests;
        std::sort(interestsTmp.begin(), interestsTmp.end());
        for (const auto& id : interestsTmp) {
            j["interests"].push_back(id);
        }
        j["sex"] = sex;
        j["birth"] = birth;
        if (premiumStart > 0) {
            j["premium"] = {{"start", premiumStart}, {"finish", premiumFinish}};
        }
    }

    // static void toJson(json& j, const EdgeList& edgeMap) {
    //     j = {};
    //     for (const auto& [id, edge] : edgeMap) {
    //         json j2;
    //         edge.toJson(j2);
    //         j2["id"] = id;
    //         j.push_back(j2);
    //     }
    // }
};

void readPremiumNow(const std::string& file) {
    if (fileExists(file)) {
        std::ifstream myfile(file);
        int value;
        myfile >> value;
        PREMIUM_NOW = value;
    } else {
        std::cout << "File " << file << " not found " << std::endl;
    }
}

bool hasPremiumNow(const AccountData& data) {
    if (data.premiumStart > 0) {
        if (data.premiumStart <= PREMIUM_NOW && PREMIUM_NOW <= data.premiumFinish) {
            return true;
        }
    }
    return false;
}

struct CompatibilityInput {
    bool premiumActivated{false};
    Status status;
    int32_t numInterests;
    int32_t ageDifference;

    AccountId accountId;

    using TSortingKey = std::tuple<bool, int32_t, int32_t, int32_t, AccountId>;

    TSortingKey getSortingKey() const {
        int32_t negativeStatus = -1 * static_cast<int32_t>(status);
        return std::make_tuple(premiumActivated, negativeStatus, numInterests, -ageDifference,
                               -accountId);
    }

    bool operator<(const CompatibilityInput& other) const {
        // less = means more compatible to take advantage of default sorting
        return getSortingKey() > other.getSortingKey();
    }
};

using InterestIdMap = IdValueMap<InterestId, std::string>;
using CountryIdMap = IdValueMap<CountryId, std::string>;
using CityIdMap = IdValueMap<CityId, std::string>;

// TODO: switch to int32_t
// using GroupKey2D = std::pair<std::string, std::string>;
// using GroupKey2D = std::string;
// using GroupResults2D = std::unordered_map<GroupKey2D, int32_t>;

// clang-format: off
enum class GroupFieldType : int8_t {
    SEX = 0,
    STATUS = 1,
    INTERESTS = 2,
    COUNTRY = 3,
    CITY = 4,
    FAKE = 5,
};
// clang-format: on

struct GroupValue {
    int32_t valueId;
    // type is used to re-construct stringValue of field back from valueId
    GroupFieldType type;

    std::string getKey() const { return std::to_string(valueId); }

    explicit GroupValue(int32_t valueId_, GroupFieldType type_) : valueId(valueId_), type(type_) {}
};

struct GroupAggregationItem {
    std::vector<GroupValue> groupValues;
    int32_t count{1};

    // key is used when storing into hash_map
    std::string key;

    const std::string& getKey() const { return key; }

    GroupAggregationItem() = default;

    GroupAggregationItem(const GroupAggregationItem& prev, const GroupValue& value) {
        groupValues.reserve(prev.groupValues.size() + 1);
        groupValues.insert(groupValues.end(), prev.groupValues.begin(), prev.groupValues.end());
        groupValues.push_back(value);

        key = prev.key + "," + value.getKey();
    }
};

// key is GroupAggregationItem::getKey()
using GroupAggregationMap = std::unordered_map<std::string, GroupAggregationItem>;

using CachedGroup = std::unordered_map<std::string, GroupAggregationMap>;
// using CachedGroup2D = std::unordered_map<std::string, GroupAggregationMap>;

using AccountIdList = std::vector<AccountId>;

void sortAccountIdsInContainer(std::vector<AccountIdList>& container) {
    for (auto& list : container) {
        // sort in reversed order
        std::sort(list.rbegin(), list.rend());
    }
}

template <class T>
void sortAccountIdsInContainer(std::unordered_map<T, AccountIdList>& container) {
    for (auto& [key, list] : container) {
        // sort in reversed order
        std::sort(list.rbegin(), list.rend());
    }
}

constexpr int NUM_SUPPORTED_BREAKDOWNS = 3;

using UsersAtIntIndex = std::vector<AccountIdList>;
using UsersAtStringIndex = std::unordered_map<std::string, AccountIdList>;

constexpr int BUCKETS_CNT = SEX_CNT * PREMIUM_CNT * STATUS_CNT;
using RecommendBuckets = std::vector<UsersAtIntIndex>;

int getRecommendBucket(SexEnum sexEnum, bool premiumNow, Status status) {
    int bin = (int)status + STATUS_CNT * ((int)premiumNow + (int)sexEnum * PREMIUM_CNT);
    MY_ASSERT(bin < BUCKETS_CNT);
    return bin;
}

struct IndexStorage {
    // std::map<AccountId, AccountData> accounts;
    // std::unordered_map<AccountId, AccountData> accounts;
    std::vector<AccountData> accountsArray;

    // debug info
    // std::vector<AccountId> allAccounts;

    InterestIdMap interestIdMap;
    CountryIdMap countryIdMap;
    CityIdMap cityIdMap;

    // single value index
    UsersAtIntIndex usersAtInterestId;
    UsersAtIntIndex usersAtStatus;
    UsersAtStringIndex usersAtCountry;
    UsersAtStringIndex usersAtCity;
    UsersAtStringIndex usersAtSex;
    UsersAtStringIndex usersAtEmailDomain;
    std::unordered_map<YearShort, AccountIdList> usersAtJoinedYear;
    std::unordered_map<YearShort, AccountIdList> usersAtBirthYear;

    // both of the following methods apply only to single value index
    void sortAccountIds() {
        sortAccountIdsInContainer(usersAtInterestId);
        sortAccountIdsInContainer(usersAtStatus);
        sortAccountIdsInContainer(usersAtCountry);
        sortAccountIdsInContainer(usersAtCity);
        sortAccountIdsInContainer(usersAtSex);
        sortAccountIdsInContainer(usersAtEmailDomain);
        sortAccountIdsInContainer(usersAtJoinedYear);
        sortAccountIdsInContainer(usersAtBirthYear);
    }

    void resetIndexes() {
        usersAtInterestId.clear();
        usersAtInterestId.resize(interestIdMap.size());

        usersAtStatus.clear();
        usersAtStatus.resize(STATUS_CNT);

        usersAtCountry.clear();
        usersAtCity.clear();
        usersAtSex.clear();
        usersAtEmailDomain.clear();
        usersAtJoinedYear.clear();
        usersAtBirthYear.clear();
    }

    // for recommend API
    // sex -> premium -> status -> interest -> Account Ids
    // 2 * 2 * 3 * 90
    RecommendBuckets recommendBuckets;

    // precomputed group results
    CachedGroup cachedGroup1D;
    CachedGroup cachedGroup2D;
    CachedGroup cachedGroup3D;
    // CachedGroup cachedGroup4D;

    CachedGroup& getCachedGroup(int size) {
        if (size == 1) {
            return cachedGroup1D;
        } else if (size == 2) {
            return cachedGroup2D;
        } else if (size == 3) {
            return cachedGroup3D;
        } else {
            MY_ASSERT(false);
        }
    }

    GroupAggregationMap* getCachedGroupResult(const std::string& cacheKey, int size) {
        if (size > 3) {
            // only up to 3D breakdowns are supported
            return nullptr;
        }
        auto& cachedGroup = getCachedGroup(size);
        return stl::mapGetPtr(cachedGroup, cacheKey);
    }

    // validation only
    std::unordered_set<std::string> emails;
};

std::string getStringValue(const GroupValue& groupValue, const IndexStorage& index) {
    switch (groupValue.type) {
        case GroupFieldType::SEX:
            return convertSexToString(static_cast<SexEnum>(groupValue.valueId));
        case GroupFieldType::STATUS:
            return convertStatusToString(static_cast<Status>(groupValue.valueId));
        case GroupFieldType::INTERESTS:
            return index.interestIdMap.getValue(groupValue.valueId);
        case GroupFieldType::CITY:
            return index.cityIdMap.getValue(groupValue.valueId);
        case GroupFieldType::COUNTRY:
            return index.countryIdMap.getValue(groupValue.valueId);
        default:
            MY_ASSERT(false);
    }
}

struct GroupSortingItem {
    //  this should be in the right order to be sorted correctly
    std::vector<std::string> fieldValues;
    int32_t count;

    // item - to extract values from
    // order - to know in which order
    // index - to know how to convert id -> string
    GroupSortingItem(const GroupAggregationItem& item, const std::vector<GroupFieldType>& order,
                     const IndexStorage& index)
        : count(item.count) {
        fieldValues.reserve(order.size());
        for (auto groupType : order) {
            bool found = false;
            for (const auto& groupValue : item.groupValues) {
                if (groupValue.type == groupType) {
                    if (!found) {
                        fieldValues.emplace_back(getStringValue(groupValue, index));
                        found = true;
                    } else {
                        std::cout << "Duplicated field found: " << static_cast<int32_t>(groupType)
                                  << std::endl;
                        MY_ASSERT(false);
                    }
                }
            }
            if (!found) {
                std::cout << "Field " << static_cast<int32_t>(groupType) << " not found"
                          << std::endl;
            }
        }
    }

    bool operator<(const GroupSortingItem& other) const {
        return std::tie(count, fieldValues) < std::tie(other.count, other.fieldValues);
    }
};
