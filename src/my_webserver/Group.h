#pragma once

#include "Base.h"
#include "Filter.h"
#include "Types.h"
#include "Util.h"

struct GroupField {
    GroupField(GroupFieldType groupFieldType_, const std::string& name_, const IndexStorage& index_)
        : name(name_), index(index_), type(groupFieldType_) {}
    const IndexStorage& index;
    std::string name;

    // is only really used to get string value for sorting
    GroupFieldType type;

    virtual ~GroupField() = default;

    const std::string& getName() { return name; }

    static std::unique_ptr<GroupField> parseField(const std::string& name,
                                                  const IndexStorage& index);
    static std::unique_ptr<GroupField> parseExtendedField(const std::string& name,
                                                          const IndexStorage& index);

    virtual bool hasSingleValue() const { return true; }
    virtual GroupValue getValue(const AccountData& data) = 0;
    virtual std::vector<GroupValue> getValues(const AccountData& data) {
        // Shouldn't be called except when overriden
        MY_ASSERT(false);
    }

    void updateWithValue(GroupValue value, int32_t size, GroupAggregationMap* map) const {
        if (size > 0) {
            GroupAggregationItem empty;
            GroupAggregationItem item(empty, value);
            item.count = size;
            const auto& key = item.getKey();
            (*map)[key] = std::move(item);
        }
    }
};

struct SexField : public GroupField {
    using GroupField::GroupField;

    GroupValue getValue(const AccountData& data) override {
        return GroupValue{static_cast<int32_t>(data.sexEnum), type};
    }
};

struct StatusField : public GroupField {
    using GroupField::GroupField;

    GroupValue getValue(const AccountData& data) override {
        return GroupValue{static_cast<int32_t>(data.status), type};
    }
};

struct InterestField : public GroupField {
    using GroupField::GroupField;

    GroupValue getValue(const AccountData& data) override {
        // Not supported
        MY_ASSERT(false);
    }

    bool hasSingleValue() const override { return false; }
    std::vector<GroupValue> getValues(const AccountData& data) override {
        std::vector<GroupValue> result;
        result.reserve(data.interests.size());
        for (const auto& interestId : data.interests) {
            if (interestId >= index.interestIdMap.size()) {
                std::cout << "Unexpected interestId: " << interestId
                          << " max = " << index.interestIdMap.size() << std::endl;
                MY_ASSERT(false);
            }
            result.emplace_back(interestId, type);
        }
        return result;
    }
};

struct CountryField : public GroupField {
    using GroupField::GroupField;

    GroupValue getValue(const AccountData& data) override {
        return GroupValue{data.countryId, type};
    }
};

struct CityField : public GroupField {
    using GroupField::GroupField;

    GroupValue getValue(const AccountData& data) override { return GroupValue{data.cityId, type}; }
};

// Now goes fake group fields for the sake of proper breakdowns:
struct BirthField : public GroupField {
    using GroupField::GroupField;

    GroupValue getValue(const AccountData& data) override {
        return GroupValue{data.birthYear, type};
    }
};

struct JoinedField : public GroupField {
    using GroupField::GroupField;

    GroupValue getValue(const AccountData& data) override {
        return GroupValue{data.joinedYear, type};
    }
};

// struct LikesField : public GroupField {
//     using GroupField::GroupField;
//
//     GroupValue getValue(const AccountData& data) override {
//         // Not supported
//         MY_ASSERT(false);
//     }
//
//     bool hasSingleValue() const override { return false; }
//     std::vector<GroupValue> getValues(const AccountData& data) override {
//         std::vector<GroupValue> result;
//         result.reserve(data.likes.size());
//         LikeEdgeIterator it{data.likes};
//         for ( ; it.valid() ; it.next()) {
//             result.emplace_back(it.getId(), type);
//         }
//         return result;
//     }
//
// };

std::unique_ptr<GroupField> GroupField::parseField(const std::string& name,
                                                   const IndexStorage& index) {
    if (name == "sex") {
        return std::make_unique<SexField>(GroupFieldType::SEX, name, index);
    } else if (name == "status") {
        return std::make_unique<StatusField>(GroupFieldType::STATUS, name, index);
    } else if (name == "interests") {
        return std::make_unique<InterestField>(GroupFieldType::INTERESTS, name, index);
    } else if (name == "country") {
        return std::make_unique<CountryField>(GroupFieldType::COUNTRY, name, index);
    } else if (name == "city") {
        return std::make_unique<CityField>(GroupFieldType::CITY, name, index);
    } else {
        THROW_ERROR("GroupField -> Unexpected field name: " + name);
    }
}

std::unique_ptr<GroupField> GroupField::parseExtendedField(const std::string& name,
                                                           const IndexStorage& index) {
    if (name == "birth") {
        return std::make_unique<BirthField>(GroupFieldType::FAKE, name, index);
    } else if (name == "joined") {
        return std::make_unique<JoinedField>(GroupFieldType::FAKE, name, index);
        // } else if (name == "likes") {
        //     return std::make_unique<LikesField>(GroupFieldType::FAKE, name, index);
    } else {
        return GroupField::parseField(name, index);
    }
}

class GroupList {
   public:
    static std::unique_ptr<GroupList> parse(const RequestParams& params,
                                            const IndexStorage& index) {
        auto result = std::make_unique<GroupList>();
        result->filters.reserve(params.size());
        for (const auto& [key, value] : params) {
            if (key == "limit" || key == "query_id") {
                // skip known fields
                continue;
            }
            if (key == "order") {
                if (value == "-1") {
                    result->increasingOrder = false;
                } else if (value == "1") {
                    result->increasingOrder = true;
                } else {
                    THROW_ERROR("GroupList -> Unexpected value of order param: " + value);
                }
            } else if (key == "keys") {
                if (value.empty()) {
                    THROW_ERROR("GroupList -> expected non-empty value of keys param");
                }
                auto parts = splitString(value, ',');
                std::unordered_set<std::string> taken;
                result->groupFieldTypes.reserve(parts.size());
                result->groupFields.reserve(parts.size());
                result->groupFieldNames.reserve(parts.size());
                for (const auto& field : parts) {
                    if (taken.count(field) > 0) {
                        THROW_ERROR("GroupList -> duplicated field: " + field);
                    }
                    taken.insert(field);

                    auto groupField = GroupField::parseField(field, index);
                    result->groupFieldTypes.push_back(groupField->type);
                    result->groupFields.push_back(std::move(groupField));
                    result->groupFieldNames.push_back(field);
                }
            } else {
                auto filter = Filter::parseGroupFilter(key, value, index);
                result->filters.push_back(std::move(filter));
            }
        }
        return result;
    }

    void updateMap(const AccountData& data, GroupAggregationMap& map, int delta) const {
        GroupAggregationItem item;
        updateMapImpl(0, item, data, map, delta);
    }

    void updateMapImpl(int32_t index, const GroupAggregationItem& item, const AccountData& data,
                       GroupAggregationMap& map, int delta) const {
        if (index == groupFields.size()) {
            const auto& key = item.getKey();
            auto valuePtr = stl::mapGetPtr(map, key);
            if (valuePtr) {
                valuePtr->count += delta;
                return;
            }
            // we set new items only when delta is positive
            MY_ASSERT(delta > 0);
            map[key] = item;
        } else {
            const auto& field = groupFields[index];
            if (field->hasSingleValue()) {
                const auto& value = field->getValue(data);
                GroupAggregationItem newItem(item, value);
                updateMapImpl(index + 1, newItem, data, map, delta);
            } else {
                for (const auto& value : field->getValues(data)) {
                    GroupAggregationItem newItem(item, value);
                    updateMapImpl(index + 1, newItem, data, map, delta);
                }
            }
        }
    }

    bool matches(AccountId accountId, const AccountData& data) {
        for (const auto& filter : filters) {
            if (!filter->matches(accountId, data)) {
                return false;
            }
        }
        return true;
    }

    int groupFieldsSize() const { return groupFields.size(); }

    // used to get canonical key of the result operation
    // see getCacheKey2D
    void sortGroupFields() {
        std::sort(groupFields.begin(), groupFields.end(),
                  [](const auto& a, const auto& b) { return a->getName() < b->getName(); });
    }

    std::string getCacheKey4D() const {
        MY_ASSERT_EQ(groupFields.size(), 4);
        return groupFields[0]->getName() + "," + groupFields[1]->getName() + "," +
               groupFields[2]->getName() + "," + groupFields[3]->getName();
    }

    std::string getCacheKey3D() const {
        MY_ASSERT_EQ(groupFields.size(), 3);
        return groupFields[0]->getName() + "," + groupFields[1]->getName() + "," +
               groupFields[2]->getName();
    }

    std::string getCacheKey2D() const {
        MY_ASSERT_EQ(groupFields.size(), 2);
        return groupFields[0]->getName() + "," + groupFields[1]->getName();
    }

    std::string getCacheKey1D() const {
        MY_ASSERT_EQ(groupFields.size(), 1);
        return groupFields[0]->getName();
    }

    std::string getCacheKeyGenericD() const {
        if (groupFields.size() == 1) {
            return getCacheKey1D();
        } else if (groupFields.size() == 2) {
            return getCacheKey2D();
        } else if (groupFields.size() == 3) {
            return getCacheKey3D();
        } else {
            std::cout << "size " << groupFields.size() << " is not suppoted" << std::endl;
            MY_ASSERT(false);
        }
    }

    GroupAggregationMap* getCachedGroupResult(IndexStorage& index) const {
        return index.getCachedGroupResult(getCacheKeyGenericD(), groupFieldsSize());
    }

    void createCachedGroup(IndexStorage& index) const {
        int size = groupFieldsSize();
        auto& cachedGroup = index.getCachedGroup(size);
        cachedGroup[getCacheKeyGenericD()] = GroupAggregationMap();
    }

    void getCombinedCacheKey(std::string& combinedKey, int& size) const {
        std::unordered_set<std::string> allNames;
        for (const auto& groupField : groupFields) {
            allNames.insert(groupField->getName());
        }
        for (const auto& filter : filters) {
            allNames.insert(filter->name);
        }
        size = allNames.size();

        std::vector<std::string> namesVec(allNames.begin(), allNames.end());
        std::sort(namesVec.begin(), namesVec.end());

        combinedKey = namesVec[0];
        for (int i = 1; i < namesVec.size(); ++i) {
            combinedKey += ',';
            combinedKey += namesVec[i];
        }
    }

    // returns index of field in groupFields vector
    // -1 otherwise
    int findMatchingGroupField(const std::string& name) const {
        for (int i = 0; i < groupFields.size(); ++i) {
            if (groupFields[i]->getName() == name) {
                return i;
            }
        }
        return INVALID_ID;
    }

    // could be changed during optimization
    std::vector<std::unique_ptr<GroupField>> groupFields;

    // doesn't change during optimizations
    std::vector<std::unique_ptr<Filter>> filters;
    std::vector<std::string> groupFieldNames;
    bool increasingOrder{true};

    // original order in which fields came (needed for sorting)
    std::vector<GroupFieldType> groupFieldTypes;
};
