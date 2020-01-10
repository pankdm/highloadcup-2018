#pragma once

#include "MemoryUsage.h"
#include "Types.h"

// Index Logs:
// ?| 0.001s | [./IndexLoader.h:31] Mem = 0.98 Mb | Allocating 1320001 accounts
// ?| 0.174s | [./IndexLoader.h:336] Mem = 383.68 Mb | Loading accounts data
// ?| 74.363s | [./IndexLoader.h:267] Mem = 907.02 Mb | Computing like hint
// ?| 75.800s | [./IndexLoader.h:447] Mem = 907.02 Mb | Applying like size hint
// ?| 75.905s | [./IndexLoader.h:456] Mem = 912.28 Mb | Building backward likes index
// ?| 84.042s | [./IndexLoader.h:325] Mem = 1204.44 Mb | Building interests index
// ?| 84.201s | [./IndexLoader.h:239] Mem = 1218.27 Mb | Building single value indexes
// ?| 84.482s | [./IndexLoader.h:252] Mem = 1259.08 Mb | Sorting single value indexes
// ?| 85.318s | [./IndexLoader.h:121] Mem = 1318.63 Mb | Precomputing group results
// ?| 88.189s | [./IndexLoader.h:148] Mem = 1318.76 Mb | Finished 1D
// ?| 110.323s | [./IndexLoader.h:174] Mem = 1320.91 Mb | Finished 2D
// ?| 171.124s | [./IndexLoader.h:203] Mem = 1496.63 Mb | Finished 3D
// ?| 171.124s | [./IndexLoader.h:89] Mem = 1496.64 Mb | Building recommend index
// ?| 172.392s | [./IndexLoader.h:257] Mem = 1508.21 Mb | Computing list of all users
// ?| 172.511s | [./IndexLoader.h:59] Mem = 1508.27 Mb | Index loading finished

// 6/15 Index Logs: (like coefficient = 1.2)
// ?| 0.001s | [./IndexLoader.h:31] Mem = 0.99 Mb | Allocating 1320001 accounts
// ?| 0.210s | [./IndexLoader.h:341] Mem = 383.70 Mb | Loading accounts data
// ?| 80.572s | [./IndexLoader.h:283] Mem = 987.48 Mb | Computing like hint
// ?| 82.015s | [./IndexLoader.h:459] Mem = 987.49 Mb | Applying like size hint
// ?| 82.171s | [./IndexLoader.h:468] Mem = 993.37 Mb | Building backward likes index
// ?| 91.823s | [./IndexLoader.h:88] Mem = 1314.07 Mb | Building set of emails
// ?| 92.755s | [./IndexLoader.h:130] Mem = 1401.08 Mb | Precomputing group results
// ?| 92.755s | [./IndexLoader.h:214] Mem = 1401.11 Mb | Precomputing group results with 55 group lists
// ?| 96.986s | [./IndexLoader.h:232] Mem = 1401.21 Mb | Finished 1D
// ?| 121.640s | [./IndexLoader.h:232] Mem = 1407.98 Mb | Finished 2D
// ?| 183.270s | [./IndexLoader.h:237] Mem = 1549.90 Mb | Finished Group Results
// ?| 183.270s | [./IndexLoader.h:248] Mem = 1549.90 Mb | Building single value indexes
// ?| 183.658s | [./IndexLoader.h:268] Mem = 1603.36 Mb | Sorting single value indexes
// ?| 183.682s | [./IndexLoader.h:96] Mem = 1603.36 Mb | Building recommend index
// ?| 183.975s | [./IndexLoader.h:75] Mem = 1610.28 Mb | Sorting account data fields
// ?| 185.143s | [./IndexLoader.h:54] Mem = 1610.28 Mb | Index loading finished

class IndexLoader {
   public:
    explicit IndexLoader(IndexStorage& index_) : index(index_) {}

    using LikeHintType = std::vector<int32_t>;

    void loadDataFromDirectory(const std::string& dir) {
        // testAccounts1.reserve(13000000);
        MY_LOG_WITH_MEMORY("Allocating " << (MAX_ACCOUNT_ID + 1) << " accounts");
        index.accountsArray.resize(MAX_ACCOUNT_ID + 1);

        loadDataFromDirectoryImpl(dir);

        if (LOAD_LIKES_INDEX) {
            LikeHintType backwardLikeHint(MAX_ACCOUNT_ID + 1, 0);
            computeLikeHint(&backwardLikeHint);
            buildBackwardLikes(backwardLikeHint);
        }

        buildListOfEmails();

        precomputeGroups(&_groupListAll);
        precomputeGroupResults(_groupListAll);

        buildSingleValueIndexes();
        buildRecommendIndex();
        // should be called after all data is built
        // e.g. backwardLikes
        sortAccountData();

        printIndexStats();
        MY_LOG_WITH_MEMORY("Index loading finished");
    }

    void rebuildIndexes() {
        // NOTE: not rebuilding group index
        // relying on incremental update
        MY_LOG_WITH_MEMORY("Rebuilding indexes");
        buildSingleValueIndexes();
        buildRecommendIndex();
        sortAccountData();
        MY_LOG_WITH_MEMORY("Rebuilding indexes finished");
    }

    bool accountExists(AccountId accountId) {
        if (isValidId(accountId) && isValidId(index.accountsArray[accountId].id)) {
            return true;
        }
        return false;
    }

    void sortAccountData() {
        MY_LOG_WITH_MEMORY("Sorting account data fields");
        FOR_EACH_ACCOUNT_ID(id) {
            TRY_GET_NONCONST_DATA(id, data);
            auto likeEdgeLess = [](const LikeEdge& a, const LikeEdge& b) {
                return a.accountId < b.accountId;
            };
            SORT_REVERSE_WITH_LAMBDA(data.backwardLikes, likeEdgeLess)
            SORT_REVERSE_WITH_LAMBDA(data.likes, likeEdgeLess);
            SORT_REVERSE(data.interests);
        }
    }

    void buildListOfEmails() {
        MY_LOG_WITH_MEMORY("Building set of emails");
        FOR_EACH_ACCOUNT_ID(id) {
            TRY_GET_CONST_DATA(id, data);
            index.emails.insert(data.email);
        }
    }

    void buildRecommendIndex() {
        MY_LOG_WITH_MEMORY("Building recommend index");

        auto numInterests = index.interestIdMap.size();
        std::vector<std::vector<int>> hint(BUCKETS_CNT, std::vector<int>(numInterests, 0));
        FOR_EACH_ACCOUNT_ID(id) {
            TRY_GET_CONST_DATA(id, data);
            for (auto interestId : data.interests) {
                auto bin = getRecommendBucket(data.sexEnum, data.hasPremiumNow, data.status);
                hint[bin][interestId]++;
            }
        }

        index.recommendBuckets.clear();
        index.recommendBuckets.resize(BUCKETS_CNT);
        for (int i = 0; i < hint.size(); ++i) {
            index.recommendBuckets[i].resize(numInterests);
            for (int j = 0; j < numInterests; ++j) {
                // std::cout << "i = " << i << ", j = " j << " hint = " << hint[i][j];
                index.recommendBuckets[i][j].reserve(hint[i][j]);
            }
        }

        FOR_EACH_ACCOUNT_ID(id) {
            TRY_GET_CONST_DATA(id, data);
            for (auto interestId : data.interests) {
                auto bin = getRecommendBucket(data.sexEnum, data.hasPremiumNow, data.status);
                index.recommendBuckets[bin][interestId].push_back(id);
            }
        }
    }

    void precomputeGroups(std::vector<GroupList>* groupListAll) {
        groupListAll->clear();

        MY_LOG_WITH_MEMORY("Precomputing group results");
#define REGULAR_GROUP_FIELDS() "sex", "status", "country", "city", "interests"
        std::vector<std::string> names = {REGULAR_GROUP_FIELDS()};
        std::vector<std::string> namesWithYears = {REGULAR_GROUP_FIELDS(), "joined", "birth"};
        std::vector<std::string> yearsOnly = {"joined", "birth"};
        // std::vector<std::string> namesAll = {REGULAR_GROUP_FIELDS(), "joined", "birth", "likes"};

        for (int i = 0; i < names.size(); ++i) {
            const auto& name = names[i];
            GroupList groupList;
            auto& groupFields = groupList.groupFields;
            groupFields.push_back(GroupField::parseField(name, index));
            // std::cout << "  running for 1 keys=" << name << std::endl;

            groupListAll->push_back(std::move(groupList));
        }

        for (int i1 = 0; i1 < names.size(); ++i1) {
            for (int i2 = i1 + 1; i2 < namesWithYears.size(); ++i2) {
                GroupList groupList;
                auto& groupFields = groupList.groupFields;
                groupFields.push_back(GroupField::parseField(names[i1], index));
                groupFields.push_back(GroupField::parseExtendedField(namesWithYears[i2], index));
                groupList.sortGroupFields();

                // std::cout << "  running for 2 keys=" << names[i1] << "," << names[i2]
                //           << std::endl;
                groupListAll->push_back(std::move(groupList));
            }
        }

        for (int i1 = 0; i1 < names.size(); ++i1) {
            for (int i2 = i1 + 1; i2 < names.size(); ++i2) {
                for (int i3 = i2 + 1; i3 < namesWithYears.size(); ++i3) {
                    GroupList groupList;
                    auto& groupFields = groupList.groupFields;
                    groupFields.push_back(GroupField::parseField(names[i1], index));
                    groupFields.push_back(GroupField::parseField(names[i2], index));
                    groupFields.push_back(
                        GroupField::parseExtendedField(namesWithYears[i3], index));
                    groupList.sortGroupFields();

                    // std::cout << "  running for 3 keys=" << names[i1] << "," << names[i2]
                    //           << std::endl;
                    groupListAll->push_back(std::move(groupList));
                }
            }
        }

        // for (int i1 = 0; i1 < names.size(); ++i1) {
        //     for (int i2 = i1 + 1; i2 < names.size(); ++i2) {
        //         for (int i3 = i2 + 1; i3 < names.size(); ++i3) {
        //             for (int i4 = 0; i4 < yearsOnly.size(); ++i4) {
        //                 GroupList groupList;
        //                 auto& groupFields = groupList.groupFields;
        //                 groupFields.push_back(GroupField::parseField(names[i1], index));
        //                 groupFields.push_back(GroupField::parseField(names[i2], index));
        //                 groupFields.push_back(GroupField::parseField(names[i3], index));
        //                 groupFields.push_back(
        //                     GroupField::parseExtendedField(yearsOnly[i4], index));
        //                 groupList.sortGroupFields();
        //
        //                 GroupAggregationMap map;
        //                 FOR_EACH_ACCOUNT_ID(id) {
        //                     TRY_GET_CONST_DATA(id, data);
        //                     groupList.updateMap(data, map);
        //                 }
        //                 std::string cacheKey = groupList.getCacheKey4D();
        //                 std::cout << "  got " << map.size() << " items, cacheKey 4D = " <<
        //                 cacheKey
        //                           << std::endl;
        //
        //                 index.cachedGroup4D[cacheKey] = map;
        //             }
        //         }
        //     }
        // }
        // MY_LOG_WITH_MEMORY("Finished 4D");

        // std::cout << "Finished, mem = " << getUsedMemoryInMb() << " Mb " << std::endl;
    }

    void precomputeGroupResults(const std::vector<GroupList>& groupListAll) {
        MY_LOG_WITH_MEMORY("Precomputing group results with " << groupListAll.size()
                                                              << " group lists");

        int currentDimension = 1;
        for (const auto& groupList : groupListAll) {
            groupList.createCachedGroup(index);

            GroupAggregationMap* map = groupList.getCachedGroupResult(index);
            constexpr int ADD_ACCOUNT = 1;

            FOR_EACH_ACCOUNT_ID(id) {
                TRY_GET_CONST_DATA(id, data);
                groupList.updateMap(data, *map, ADD_ACCOUNT);
            }

            std::string cacheKey = groupList.getCacheKeyGenericD();
            MY_LOG(INFO_LEVEL, "  got " << map->size() << " items, cacheKey " << cacheKey);

            if (groupList.groupFieldsSize() != currentDimension) {
                MY_LOG_WITH_MEMORY("Finished " << currentDimension << "D");
                currentDimension = groupList.groupFieldsSize();
            }
        }

        MY_LOG_WITH_MEMORY("Finished Group Results");
    }

    void updateCachedGroupResult(const AccountData& data, int delta) {
        for (auto& groupList : _groupListAll) {
            GroupAggregationMap* map = groupList.getCachedGroupResult(index);
            groupList.updateMap(data, *map, delta);
        }
    }

    void buildSingleValueIndexes() {
        MY_LOG_WITH_MEMORY("Building single value indexes");

        index.resetIndexes();

        FOR_EACH_ACCOUNT_ID(id) {
            TRY_GET_CONST_DATA(id, data);

            index.usersAtCity[data.city].push_back(id);
            index.usersAtCountry[data.country].push_back(id);
            index.usersAtSex[data.sex].push_back(id);
            index.usersAtEmailDomain[data.emailDomain].push_back(id);
            index.usersAtStatus[static_cast<int8_t>(data.status)].push_back(id);
            index.usersAtJoinedYear[data.joinedYear].push_back(id);
            index.usersAtBirthYear[data.birthYear].push_back(id);

            for (const auto& interestId : data.interests) {
                index.usersAtInterestId[interestId].push_back(id);
            }
        }

        MY_LOG_WITH_MEMORY("Sorting single value indexes");
        index.sortAccountIds();
    }

    // void buildListOfAllUsers() {
    //     MY_LOG_WITH_MEMORY("Computing list of all users");
    //     FOR_EACH_ACCOUNT_ID(id) {
    //         TRY_GET_CONST_DATA(id, data);
    //         if (data.interests.size() >= 6) {
    //             index.allAccounts.push_back(id);
    //         }
    //     }
    // }

    void computeLikeHint(LikeHintType* backwardLikeHint) {
        MY_LOG_WITH_MEMORY("Computing like hint");
        FOR_EACH_ACCOUNT_ID(id) {
            TRY_GET_CONST_DATA(id, data);
            for (const auto& edge : data.likes) {
                if (accountExists(edge.accountId)) {
                    (*backwardLikeHint)[edge.accountId]++;
                }
            }
        }
    }

    void printIndexStats() {
        std::cout << "Total accounts loaded: " << index.accountsArray.size() << std::endl;

        int64_t totalInterests = 0;
        int64_t totalLikes = 0;
        int64_t totalDuplicatedLikes = 0;
        int32_t minTs = 0;
        int32_t maxTs = 0;

        AccountId minId = -1;
        AccountId maxId = -1;

        FOR_EACH_ACCOUNT_ID(id) {
            TRY_GET_CONST_DATA(id, data);
            totalInterests += data.interests.size();
            totalLikes += data.likes.size();
            for (const auto& edge : data.likes) {
                if (minTs == 0) {
                    minTs = edge.ts;
                }
                if (maxTs == 0) {
                    maxTs = edge.ts;
                }

                minTs = std::min(minTs, edge.ts);
                maxTs = std::max(maxTs, edge.ts);

                // totalDuplicatedLikes += (edge.nTs - 1);
            }

            minId = minId == -1 ? id : std::min(minId, data.id);
            maxId = maxId == -1 ? id : std::max(maxId, data.id);
        }
        std::cout << "Inserted total likes: " << totalLikes << std::endl;
        std::cout << "Inserted total duplicated likes: " << totalDuplicatedLikes << std::endl;
        std::cout << "min like ts = " << minTs << std::endl;
        std::cout << "max like ts = " << maxTs << std::endl;
        std::cout << "Inserted total interests: " << totalInterests << std::endl;
        std::cout << "Min account Id = " << minId << std::endl;
        std::cout << "Max account Id = " << maxId << std::endl;

        std::cout << "Unique interests: " << index.usersAtInterestId.size() << std::endl;
        std::cout << "Unique countries: " << index.usersAtCountry.size() << std::endl;
        std::cout << "Unique cities: " << index.usersAtCity.size() << std::endl;
    }

    void loadDataFromDirectoryImpl(const std::string& dir) {
        MY_LOG_WITH_MEMORY("Loading accounts data");
        std::vector<std::string> files;
        readDirectory(dir, &files);
        int counter = 0;
        for (const auto& file : files) {
            if (endsWith(file, ".json")) {
                // std::cout << "found " << file << std::endl;
                std::string fullPath = dir + "/" + file;

                if (counter % 10 == 0) {
                    MY_LOG_WITH_MEMORY(counter << "/" << files.size() << " loading from "
                                               << fullPath);
                }
                loadData(fullPath);
            }
            ++counter;
        }
    }

    void loadData(const std::string& fileName) {
        std::ifstream input(fileName);
        json data;
        input >> data;

        int count = data["accounts"].size();
        for (const auto& acc : data["accounts"]) {
            auto accountId = acc["id"].get<int>();
            MY_ASSERT(isValidId(accountId));
            // jsonDump[accountId] = acc;

            auto& data = index.accountsArray[accountId];
            data.id = accountId;
            // std::cout << std::setw(4) << acc << std::endl;
            loadAccountData(acc, data);
            // std::cout << "id=" << accountId << " loaded email: " << data.email << std::endl;
        }
    }

    void loadAccountData(const json& j, AccountData& data) {
        loadStringData(j, "email", &data.email);
        data.emailDomain = getEmailDomain(data.email);

        loadStringData(j, "fname", &data.fname);
        loadStringData(j, "sname", &data.sname);
        loadStringData(j, "phone", &data.phone);

        loadStringData(j, "sex", &data.sex);
        data.sexEnum = convertStringToSex(data.sex);

        std::string statusStr;
        loadStringData(j, "status", &statusStr);
        if (!statusStr.empty()) {
            data.status = convertStringToStatus(statusStr);
        }

        loadStringData(j, "country", &data.country);
        loadStringData(j, "city", &data.city);

        data.countryId = index.countryIdMap.getOrCreateId(data.country);
        data.cityId = index.cityIdMap.getOrCreateId(data.city);

        loadIntData(j, "birth", &data.birth);
        loadIntData(j, "joined", &data.joined);

        data.birthYear = (getYearFromTimestamp(data.birth) - BASE_YEAR);
        data.joinedYear = (getYearFromTimestamp(data.joined) - BASE_YEAR);

        if (j.count("premium") > 0) {
            const auto& jp = j["premium"];
            loadIntData(jp, "start", &data.premiumStart);
            loadIntData(jp, "finish", &data.premiumFinish);
            data.hasPremiumNow = hasPremiumNow(data);
        }

        if (LOAD_LIKES_INDEX) {
            // likes
            if (j.count("likes") > 0) {
                auto size = getGoodLikesSize(j["likes"].size());
                data.likes.reserve(size);
                for (const auto& like : j["likes"]) {
                    auto accountId = like["id"].get<int>();
                    // if (!isValidId(accountId)) {
                    //     continue;
                    // }

                    auto& edge = data.likes.emplace_back();
                    edge.accountId = accountId;
                    edge.ts = like["ts"].get<int>();
                }
            }
        }

        // interests
        if (j.count("interests") > 0) {
            data.interests.clear();
            data.interests.reserve(j["interests"].size());
            for (const auto& interestJson : j["interests"]) {
                std::string interest = interestJson.get<std::string>();
                auto interestId = index.interestIdMap.getOrCreateId(interest);
                data.interests.push_back(interestId);
            }
        }
    }

    static void loadStringData(const json& j, const std::string& key, std::string* field) {
        if (j.count(key) > 0) {
            *field = j[key].get<std::string>();
        }
    }

    static void loadIntData(const json& j, const std::string& key, int32_t* field) {
        if (j.count(key) > 0) {
            *field = j[key].get<int32_t>();
        }
    }

    void buildBackwardLikes(const LikeHintType& backwardLikeHint) {
        auto& accountsArray = index.accountsArray;
        MY_LOG_WITH_MEMORY("Applying like size hint");
        FOR_EACH_ACCOUNT_ID(id) {
            if (!isValidId(accountsArray[id].id)) {
                continue;
            }
            auto size = getGoodLikesSize(backwardLikeHint[id]);
            accountsArray[id].backwardLikes.reserve(size);
        }

        MY_LOG_WITH_MEMORY("Building backward likes index");
        FOR_EACH_ACCOUNT_ID(id) {
            TRY_GET_CONST_DATA(id, data);
            for (const auto& edge : data.likes) {
                if (!accountExists(edge.accountId)) {
                    continue;
                }
                MY_ASSERT(isValidId(accountsArray[edge.accountId].id));
                auto& backwardEdge = accountsArray[edge.accountId].backwardLikes.emplace_back();
                backwardEdge.accountId = id;
                backwardEdge.ts = edge.ts;
            }
        }
    }

   private:
    IndexStorage& index;
    std::vector<GroupList> _groupListAll;
};
