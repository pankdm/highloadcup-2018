#pragma once

#include "Base.h"
#include "Filter.h"
#include "Group.h"
#include "IndexLoader.h"
#include "MemoryUsage.h"
#include "QueryOptimizer.h"
#include "Timer.h"
#include "Types.h"
#include "Util.h"
#include "Semaphore.h"


class Server {
   public:
    Server() : loader(index) {}

    void loadDataFromDirectory(const std::string& dir) { loader.loadDataFromDirectory(dir); }

    void rebuildIndexes() { loader.rebuildIndexes(); }

    void selectFields(AccountId id, json& j, const SelectedFields& fields) {
        if (!isValidId(id)) {
            return;
        }
        auto* data = &index.accountsArray[id];
        if (!isValidId(data->id)) {
            return;
        }

        maybeSelectStringField(fields, data->fname, "fname", j);
        maybeSelectStringField(fields, data->sname, "sname", j);
        maybeSelectStringField(fields, data->email, "email", j);
        maybeSelectStringField(fields, data->phone, "phone", j);

        maybeSelectStringField(fields, convertStatusToString(data->status), "status", j);
        maybeSelectStringField(fields, data->sex, "sex", j);

        maybeSelectStringField(fields, data->country, "country", j);
        maybeSelectStringField(fields, data->city, "city", j);

        if (stl::contains(fields, "birth")) {
            j["birth"] = data->birth;
        }

        if (stl::contains(fields, "premium")) {
            if (data->premiumStart > 0) {
                j["premium"] = {{"start", data->premiumStart}, {"finish", data->premiumFinish}};
            }
        }
    }

    void maybeSelectStringField(const SelectedFields& selected, const std::string& data,
                                const std::string& name, json& j) {
        if (stl::contains(selected, name)) {
            saveDataIfPresent(data, j, name);
        }
    }

    static void saveAccountDataForLikes(const AccountData& data, json& j) {
        // TODO: verify that we don't get empty fields in input data
        saveDataIfPresent(data.fname, j, "fname");
        saveDataIfPresent(data.sname, j, "sname");
        saveDataIfPresent(data.email, j, "email");
        saveDataIfPresent(convertStatusToString(data.status), j, "status");
    }

    void saveForRecommendAPI(const AccountData& data, json& j) {
        saveDataIfPresent(data.email, j, "email");
        saveDataIfPresent(convertStatusToString(data.status), j, "status");
        saveDataIfPresent(data.fname, j, "fname");
        saveDataIfPresent(data.sname, j, "sname");

        j["birth"] = data.birth;
        if (data.premiumStart > 0) {
            j["premium"] = {{"start", data.premiumStart}, {"finish", data.premiumFinish}};
        }
    }

    static void saveDataIfPresent(const std::string& data, json& j, const std::string& key) {
        if (!data.empty()) {
            j[key] = data;
        }
    }

    bool accountExists(AccountId accountId) {
        if (isValidId(accountId) && isValidId(index.accountsArray[accountId].id)) {
            return true;
        }
        return false;
    }

    void suggestAPI(json& j, AccountId accountId, int32_t limit, const RequestParams& params) {
        auto ids = suggestByLikes(accountId, limit, params);

        // construct response
        j["accounts"] = json::array();
        for (const auto& resultId : ids) {
            // debug check - probably not need in PROD
            if (!accountExists(resultId)) {
                continue;
            }
            MY_ASSERT(accountExists(resultId));

            json object;
            object["id"] = resultId;
            Server::saveAccountDataForLikes(index.accountsArray[resultId], object);

            j["accounts"].push_back(object);
        }
    }

    std::vector<AccountId> suggestByLikes(AccountId myAccountId, int32_t limit,
                                          const RequestParams& params) {
        auto locationFilter = LocationFilter::parse(params, index);
        MY_ASSERT(isValidAccount(myAccountId));
        const auto* data = &index.accountsArray[myAccountId];
        // go through all likes of given myAccountId
        // and accumulate similarity over ids
        // A --> B <-- [X, Y]
        // A --> C <-- [Z]
        std::unordered_map<AccountId, double> similarity;
        std::unordered_set<AccountId> likedByMe;

        for (const auto& edge : data->likes) {
            // std::cout << " looking at next: " << nextId << std::endl;
            if (!isValidAccount(edge.accountId)) {
                // account doesn't exist for some reason
                // maybe deleted?
                // probably shouldn't happen if we clean things properly
                continue;
            }
            auto* nextData = &index.accountsArray[edge.accountId];

            likedByMe.insert(edge.accountId);
            // find all potential candidates that also liked "B"
            for (const auto& backwardEdge : nextData->backwardLikes) {
                // std::cout << nextId << " was also liked by " << similarUserId
                // << std::endl;
                if (!isValidAccount(backwardEdge.accountId)) {
                    continue;
                }
                if (backwardEdge.accountId == myAccountId) {
                    // skip yourself
                    continue;
                }
                // TODO: use average
                double likeDiff = std::abs(edge.ts - backwardEdge.ts);
                double value = (likeDiff < constants::EPSILON) ? 1.0 : 1.0 / likeDiff;
                similarity[backwardEdge.accountId] += value;
            }
        }

        // sort most similar users in reversed order
        std::vector<std::pair<double, AccountId>> mostSimilar;
        mostSimilar.reserve(similarity.size());
        for (const auto& [otherId, value] : similarity) {
            mostSimilar.emplace_back(value, otherId);
        }
        std::sort(mostSimilar.rbegin(), mostSimilar.rend());

        // now iterate through results until we find required number of entries
        std::vector<AccountId> finalResult;
        std::unordered_set<AccountId> taken;
        for (const auto& [value, similarUserId] : mostSimilar) {
            auto* similarUserData = &index.accountsArray[similarUserId];
            if (!isValidId(similarUserData->id)) {
                // probably shouldn't happen
                continue;
            }
            // std::cout << "getting likes from user " << similarUserId << "
            // value = "
            // << value << std::endl;
            if (locationFilter && !locationFilter->matches(similarUserId, *similarUserData)) {
                continue;
            }
            std::vector<AccountId> perSimilarUserResults;
            for (const auto& [id, edge] : similarUserData->likes) {
                if (likedByMe.count(id) > 0) {
                    // skip the ones already liked by me
                    continue;
                }
                if (taken.count(id) > 0) {
                    // don't take the same user twice
                    continue;
                }
                taken.insert(id);
                perSimilarUserResults.push_back(id);
            }
            // sort by descending likes
            if (!perSimilarUserResults.empty()) {
                std::sort(perSimilarUserResults.rbegin(), perSimilarUserResults.rend());
                finalResult.insert(finalResult.end(), perSimilarUserResults.begin(),
                                   perSimilarUserResults.end());
                if (finalResult.size() >= limit) {
                    break;
                }
            }
        }
        if (finalResult.size() > limit) {
            finalResult.resize(limit);
        }
        return finalResult;
    }

    void recommendAPI(json& j, AccountId accountId, int32_t limit, const RequestParams& params) {
        std::vector<AccountId> ids;
        if (ENABLE_RECOMMEND_API) {
            ids = recommendForUser(accountId, limit, params);
        }
        // if (DEBUG_MEMORY_LEAK) {
        //     ids = hackyRecommendForUser(accountId, limit, params);
        // }

        // construct response
        j["accounts"] = json::array();
        for (const auto& resultId : ids) {
            MY_ASSERT(accountExists(resultId));

            json object;
            object["id"] = resultId;
            saveForRecommendAPI(index.accountsArray[resultId], object);

            j["accounts"].push_back(object);
        }
    }

    void appendFromBreakdown(AccountId myId, const AccountData& myData,
                             const UsersAtIntIndex& usersAtInterestId,
                             std::unique_ptr<Filter>& locationFilter, int limit,
                             std::vector<AccountId>& finalResult) {
        std::unordered_map<AccountId, int32_t> numCommonInterests;
        for (const auto& interestId : myData.interests) {
            MY_ASSERT(interestId < usersAtInterestId.size());
            for (const auto& userId : usersAtInterestId[interestId]) {
                if (userId == myId) {
                    // skip yourself
                    continue;
                }
                numCommonInterests[userId] += 1;
            }
        }

        std::vector<CompatibilityInput> compatiblity;
        for (const auto& [userId, numInterests] : numCommonInterests) {
            const auto& userData = index.accountsArray[userId];
            MY_ASSERT(isValidId(userData.id));
            if (locationFilter && !locationFilter->matches(userId, userData)) {
                // take only the ones that match country/city
                continue;
            }

            auto& input = compatiblity.emplace_back();
            input.accountId = userId;
            input.numInterests = numInterests;

            input.ageDifference = std::abs(myData.birth - userData.birth);
            input.status = userData.status;
            input.premiumActivated = userData.hasPremiumNow;
        }
        std::sort(compatiblity.begin(), compatiblity.end());

        //
        for (int i = 0; finalResult.size() < limit && i < compatiblity.size(); ++i) {
            const auto& c = compatiblity[i];
            finalResult.push_back(c.accountId);
        }
    }

    std::vector<AccountId> recommendForUser(AccountId myId, int32_t limit,
                                            const RequestParams& params) {
        // std::cout << std::endl;
        // std::cout << "== processing recommend for user " << myAccountId  << " == " <<
        // std::endl;
        auto locationFilter = LocationFilter::parse(params, index);

        const auto& myData = index.accountsArray[myId];
        MY_ASSERT(isValidId(myData.id));

        std::vector<AccountId> finalResult;
        finalResult.reserve(limit);

        auto matchingSex = getOppositeSexEnum(myData.sexEnum);
        for (int premiumNow = 1; premiumNow >= 0; --premiumNow) {
            for (int status = 0; status < STATUS_CNT; ++status) {
                auto bin =
                    getRecommendBucket(matchingSex, (bool)premiumNow, static_cast<Status>(status));
                const auto& usersAtInterestId = index.recommendBuckets[bin];
                appendFromBreakdown(myId, myData, usersAtInterestId, locationFilter, limit,
                                    finalResult);
                if (finalResult.size() >= limit) {
                    return finalResult;
                }
            }
        }
        return finalResult;
    }

    // std::vector<AccountId> hackyRecommendForUser(AccountId myAccountId, int32_t limit,
    //                                              const RequestParams& params) {
    //     // std::cout << std::endl;
    //     // std::cout << "== processing recommend for user " << myAccountId  << " == " <<
    //     // std::endl;
    //     auto locationFilter = LocationFilter::parse(params, index);
    //     const auto* myData = &index.accountsArray[myAccountId];
    //     MY_ASSERT(isValidId(myData->id));
    //
    //     int counter = 0;
    //     std::unordered_map<AccountId, int32_t> numCommonInterests;
    //     for (const auto& interestId : myData->interests) {
    //         MY_ASSERT(interestId < index.usersAtInterestId.size());
    //         int counter = 0;
    //         for (const auto& userId : index.usersAtInterestId[interestId]) {
    //             if (userId == myAccountId) {
    //                 // skip yourself
    //                 continue;
    //             }
    //             if (3 * counter > index.usersAtInterestId[interestId].size()) {
    //                 break;
    //             }
    //             ++counter;
    //
    //             numCommonInterests[userId] += 1;
    //         }
    //     }
    //     auto matchingSex = getOppositeSex(myData->sex);
    //
    //     std::vector<AccountId> finalResult;
    //     finalResult.reserve(limit);
    //
    //     return finalResult;
    // }

    // returns true if success
    bool tryOptimizedFilterQuery(FilterList* filterList, std::vector<AccountId>* ids) {
        auto optimizedFilter = rewriteFilters(filterList->filters);
        if (!optimizedFilter) {
            return false;
        }

        bool checkLookup = false;
        if (optimizedFilter->lookupFilter->name == "interests") {
            checkLookup = true;
        }
        auto idIterator = optimizedFilter->lookupFilter->findRemainingItems();
        // std::cout << "running optimized query with lookup, size = " << idIterator->size();
        for (; idIterator->valid(); idIterator->next()) {
            auto id = idIterator->getId();
            const auto& data = index.accountsArray[id];
            if (isValidId(data.id)) {
                if (checkLookup) {
                    // skip the ones that doesn't match lookup
                    if (!optimizedFilter->lookupFilter->matches(id, data)) {
                        continue;
                    }
                }
                if (optimizedFilter->matches(id, data)) {
                    ids->push_back(id);
                    if (ids->size() >= filterList->limit) {
                        break;
                    }
                }
            }
        }
        return true;
    }

    void filterAPI(json& j, const RequestParams& params, int32_t limit) {
        auto filterList = FilterList::parse(params, index);
        filterList->limit = limit;

        std::vector<AccountId> ids;
        ids.reserve(limit);

        if (!tryOptimizedFilterQuery(filterList.get(), &ids)) {
            // default case
            // iterate through accounts backwards
            for (int id = MAX_ACCOUNT_ID; id > 0; --id) {
                TRY_GET_CONST_DATA(id, data);
                if (filterList->matches(id, data)) {
                    ids.push_back(id);
                    if (ids.size() >= limit) {
                        break;
                    }
                }
            }
        }

        auto selectedFields = filterList->selectedFields;
        // additionally print email
        selectedFields.insert("email");

        // construct response
        j["accounts"] = json::array();
        // j["accounts"].reserve(limit);
        for (const auto& resultId : ids) {
            MY_ASSERT(accountExists(resultId));

            json object;
            object["id"] = resultId;
            selectFields(resultId, object, selectedFields);

            j["accounts"].push_back(object);
        }
    }

    void groupAPI(json& j, const RequestParams& params, int32_t limit) {
        auto groupList = GroupList::parse(params, index);
        GroupAggregationMap map;

        GroupOptimizer groupOptimizer(index, &map);
        groupOptimizer.aggregateIntoMap(groupList.get());

        std::vector<GroupSortingItem> result;
        result.reserve(map.size());
        for (const auto& [key, item] : map) {
            result.emplace_back(item, groupList->groupFieldTypes, index);
        }

        if (groupList->increasingOrder) {
            std::sort(result.begin(), result.end());
        } else {
            std::sort(result.rbegin(), result.rend());
        }

        j["groups"] = json::array();
        // j["groups"].reserve(limit);
        int numFound = 0;
        for (int i = 0; numFound < limit && i < result.size(); ++i) {
            const auto& item = result[i];
            json object;
            if (item.count == 0) {
                continue;
            }

            object["count"] = item.count;
            MY_ASSERT(item.fieldValues.size() == groupList->groupFieldNames.size());
            // fill all fields of response
            for (int index = 0; index < item.fieldValues.size(); ++index) {
                const auto& value = item.fieldValues[index];
                if (!value.empty()) {
                    const auto& fieldName = groupList->groupFieldNames[index];
                    object[fieldName] = value;
                }
            }
            ++numFound;
            j["groups"].push_back(object);
        }
    }

    bool isValidAccount(AccountId id) {
        return isValidId(id) && isValidId(index.accountsArray[id].id);
    }

    bool getInt32Field(const json& j, const std::string& key, int* ptr) {
        auto it = j.find(key);
        if (it == j.end()) {
            MY_LOG(DEBUG_LEVEL, "key " << key << " not found");
            return false;
        }
        if (!it->is_number()) {
            MY_LOG(DEBUG_LEVEL, "key " << key << " not a number");
            return false;
        }
        *ptr = it->get<int32_t>();
        return true;
    }

    bool getAccountIdField(const json& j, const std::string& key, int* ptr) {
        if (!getInt32Field(j, key, ptr)) {
            MY_LOG(DEBUG_LEVEL, "key " << key << " wans't parsed as Int32");
            return false;
        }
        if (!accountExists(*ptr)) {
            MY_LOG(DEBUG_LEVEL, "Account " << *ptr << " doesn't exist");
            return false;
        }
        return true;
    }

    bool validateUpdateJSON(const json& j) {
        if (j.count("sex") > 0) {
            std::string sex = j["sex"].get<std::string>();
            validateSexValue(sex);
        }
        if (j.count("status") > 0) {
            std::string status = j["status"].get<std::string>();
            convertStringToStatus(status);
        }
        if (j.count("premium") > 0) {
            if (!j["premium"].is_object()) {
                MY_LOG(INFO_LEVEL, "Wrong premium field format");
                return false;
            }
        }
        if (j.count("joined") > 0) {
            if (!j["joined"].is_number()) {
                MY_LOG(INFO_LEVEL, "Wrong joined field format");
                return false;
            }
        }
        if (j.count("birth") > 0) {
            if (!j["birth"].is_number()) {
                MY_LOG(INFO_LEVEL, "Wrong birth field format");
                return false;
            }
        }

        if (j.count("likes")) {
            for (const auto& obj : j["likes"]) {
                int32_t ts;
                if (!getInt32Field(obj, "ts", &ts)) {
                    return false;
                }
            }
        }

        if (j.count("email") > 0) {
            std::string email = j["email"].get<std::string>();
            // get email domain to validate it
            getEmailDomain(email);
            if (stl::contains(index.emails, email)) {
                MY_LOG(INFO_LEVEL, "Duplicated email: " + email);
                return false;
            }
        }
        return true;
    }

    // returns true if success
    bool likesAPI(const json& j) {
        // std::cout << " checking likes field" << std::endl;

        auto inner = j.find("likes");
        if (inner == j.end()) {
            return false;
        }

        std::vector<LikeInput> likes;
        likes.reserve(inner->size());
        // validate first
        for (const auto& obj : *inner) {
            // std::cout << " iterating through obj " << obj << std::endl;
            if (obj.size() != 3) {
                MY_LOG(DEBUG_LEVEL, "obj.size() == " << obj.size());
                return false;
            }
            auto& input = likes.emplace_back();
            if (getAccountIdField(obj, "likee", &input.likee) &&
                getAccountIdField(obj, "liker", &input.liker) &&
                getInt32Field(obj, "ts", &input.ts)) {
            } else {
                MY_LOG(DEBUG_LEVEL, "Haven't parsed one of the fields");
                return false;
            }
        }

        ScopedSemaphore scope(_updateMutex);
        // if everything is ok process those likes:
        for (const auto& l : likes) {
            index.accountsArray[l.liker].likes.emplace_back(l.likee, l.ts);
            index.accountsArray[l.likee].backwardLikes.emplace_back(l.liker, l.ts);
        }

        return true;
    }

    bool newAPI(const json& j) {
        AccountId id;
        if (!getInt32Field(j, "id", &id)) {
            return false;
        }
        if (!isValidId(id)) {
            // this shouldn't happen unless the ids come out of range
            MY_LOG(ERROR_LEVEL, "Received unexpected id: " << id);
            return false;
        }

        // if (j.count("email") > 0) {
        //     std::string email = j["email"].get<std::string>();
        //     // std::cout << "id=" << data.id << " new email: " << email << std::endl;
        //
        //     // keep track of emails for validation
        //     index.emails.insert(email);
        //     index.accountsArray[data.id].email = email;
        // }
        //
        // index.accountsArray[data.id].id = data.id;
        ScopedSemaphore scope(_updateMutex);
        auto& data = index.accountsArray[id];
        data.id = id;
        loader.loadAccountData(j, data);

        // backward likes need to updated manually
        for (const auto& likeEdge : data.likes) {
            index.accountsArray[likeEdge.accountId].backwardLikes.emplace_back(id, likeEdge.ts);
        }

        // TODO: use mutex?
        index.emails.insert(data.email);

        loader.updateCachedGroupResult(data, ADD_ACCOUNT);
        // if (data.id > maxAccountId) {
        //     maxAccountId = data.id;
        //     MY_LOG(INFO_LEVEL, "now maxAccountId = " << maxAccountId);
        // }
        return true;
    }

    bool updateAPI(AccountId id, const json& j) {
        if (!validateUpdateJSON(j)) {
            return false;
        }

        ScopedSemaphore scope(_updateMutex);
        if (j.count("email") > 0) {
            std::string email = j["email"].get<std::string>();
            const std::string prevEmail = index.accountsArray[id].email;
            // std::cout << "id=" << id << " update email: " << email
            //     << " before: " << prevEmail << std::endl;

            // keep track of unique emails for validation
            // TODO: use mutex?
            index.emails.erase(prevEmail);
            index.emails.insert(email);
        }

        auto& data = index.accountsArray[id];

        loader.updateCachedGroupResult(data, REMOVE_ACCOUNT);

        loader.loadAccountData(j, data);
        // backward likes need to updated manually
        for (const auto& likeEdge : data.likes) {
            index.accountsArray[likeEdge.accountId].backwardLikes.emplace_back(id, likeEdge.ts);
        }

        loader.updateCachedGroupResult(data, ADD_ACCOUNT);

        return true;
    }

    void verifyCountryCounter() {
        json j;
        RequestParams p;
        p["order"] = "-1";
        p["keys"] = "country";
        int32_t limit = 1;
        groupAPI(j, p, limit);
        int count = j["groups"][0]["count"].get<int32_t>();

        int naiveCount = 0;
        FOR_EACH_ACCOUNT_ID(id) {
            TRY_GET_CONST_DATA(id, data);
            if (data.country.empty()) {
                ++naiveCount;
            }
        }
        MY_LOG(ALWAYS_LOG, "Counters: got " << count << ", Expected " << naiveCount);
        if (count != naiveCount) {
            MY_LOG(ALWAYS_LOG, "MISMATCH!!! " << count << " != " << naiveCount);
        }
    }

    // data
    IndexStorage index;

    // using semaphor counter = 1 to simulate mutex
    Semaphore _updateMutex{1};

    // loader
    IndexLoader loader;
    static constexpr int ADD_ACCOUNT = 1;
    static constexpr int REMOVE_ACCOUNT = -1;

    // debug json
    std::unordered_map<AccountId, json> jsonDump;
};
