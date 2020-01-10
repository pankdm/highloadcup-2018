#pragma once

#include "Filter.h"

// rewrites the query into single lookup (by smallest field) + the rest of the filters
std::unique_ptr<OptimizedFilter> rewriteFilters(std::vector<std::unique_ptr<Filter>>& filters) {
    MY_LOG(INFO_LEVEL, "rewriting query with " << filters.size() << " filters ");
    int minOutputSize = 0;
    int bestIndex = -1;
    for (int i = 0; i < filters.size(); ++i) {
        if (filters[i]->supportsLookup()) {
            MY_LOG(INFO_LEVEL, "  estimating size " << filters[i]->name);
            int outputSize = filters[i]->estimateOutputSize();
            MY_LOG(INFO_LEVEL, "    filter " << filters[i]->name << ", size = " << outputSize);
            if (bestIndex == -1 || outputSize < minOutputSize) {
                minOutputSize = outputSize;
                bestIndex = i;
            }
        }
    }

    if (bestIndex != -1) {
        MY_LOG(INFO_LEVEL, "  chose " << filters[bestIndex]->name << " filter");
        auto result = std::make_unique<OptimizedFilter>();
        result->filters.reserve(filters.size() - 1);
        for (int i = 0; i < filters.size(); ++i) {
            if (i == bestIndex) {
                result->lookupFilter = std::move(filters[i]);
            } else {
                result->filters.push_back(std::move(filters[i]));
            }
        }
        return result;
    }
    return nullptr;
}

struct GroupOptimizer {
    IndexStorage& index;
    GroupAggregationMap* map;
    static constexpr int ADD_ACCOUNT = 1;

    explicit GroupOptimizer(IndexStorage& index_, GroupAggregationMap* map_)
        : index(index_), map(map_) {}

    bool tryLookupOptimization(GroupList* groupList) {
        // only works when there is filter
        if (groupList->filters.empty()) {
            return false;
        }

        auto optimizedFilter = rewriteFilters(groupList->filters);
        if (!optimizedFilter) {
            return false;
        }
        auto idIterator = optimizedFilter->lookupFilter->findRemainingItems();
        for (; idIterator->valid(); idIterator->next()) {
            auto id = idIterator->getId();
            const auto& data = index.accountsArray[id];
            if (data.id > 0) {
                if (optimizedFilter->matches(id, data)) {
                    groupList->updateMap(data, *map, ADD_ACCOUNT);
                }
            }
        }
        return true;
    }

    bool tryNoFilterCachedOptimization(GroupList* groupList) {
        if (groupList->filters.empty()) {
            // No fiters
            if (groupList->groupFields.size() <= NUM_SUPPORTED_BREAKDOWNS) {
                auto ptr = getCachedGroupResult(groupList);
                *map = *ptr;
                return true;
            }
        }
        return false;
    }

    GroupAggregationMap* getCachedGroupResult(const std::string& cacheKey, int size) {
        return index.getCachedGroupResult(cacheKey, size);
    }

    GroupAggregationMap* getCachedGroupResult(GroupList* groupList) {
        return groupList->getCachedGroupResult(index);
    }

    bool isCachedBreakdown(const std::string& name) {
        if (name == "sex" || name == "status" || name == "interests" || name == "country" ||
            name == "city" || name == "joined" || name == "birth") {
            return true;
        }
        return false;
    }

    // We use optimization that
    //    GROUP(A, B) + FILTER(C = c0)
    //    GROUP(A, B, C) + FILTER(C = c0) + DROP(C)
    // are the same queries
    bool tryFilterBreakdownIsCachedOptimization(GroupList* groupList) {
        // only implemented for single and double filters now
        const auto& filters = groupList->filters;
        if (filters.size() > 2) {
            return false;
        }
        for (const auto& filter : filters) {
            const auto& name = filter->name;
            if (!isCachedBreakdown(name)) {
                return false;
            }
        }

        std::string combinedKey;
        int size;
        groupList->getCombinedCacheKey(combinedKey, size);
        auto ptr = getCachedGroupResult(combinedKey, size);
        if (!ptr) {
            return false;
        }

        for (const auto& filter : filters) {
            const auto& name = filter->name;
            // try to find the filter among breakdowns
            int matchingIndex = groupList->findMatchingGroupField(name);
            if (matchingIndex == -1) {
                MY_LOG(INFO_LEVEL, "adding field " << name << " as a new breakdown");
                // not found - add field as breakdown
                groupList->groupFields.emplace_back(GroupField::parseExtendedField(name, index));
            } else {
                // found -- using existing breakdowns
                // only need to re-compute the matchingIndex to point into right place
                // se below
            }
        }

        // get to canonical form
        groupList->sortGroupFields();

        // remember matcing indexes
        std::vector<int> matchingIndexes;
        matchingIndexes.reserve(filters.size());
        for (const auto& filter : filters) {
            const auto& name = filter->name;
            int matchingIndex = groupList->findMatchingGroupField(name);
            MY_ASSERT(matchingIndex != INVALID_ID);
            matchingIndexes.push_back(matchingIndex);
        }

        if (groupList->groupFields.size() > NUM_SUPPORTED_BREAKDOWNS) {
            // only certain amount of breakdowns is currently supported
            return false;
        }

        MY_LOG(INFO_LEVEL, "using cached group optimization with " << groupList->groupFields.size()
                                                                   << " fields");
        // std::cout <<  << std::endl;
        // auto ptr = getCachedGroupResult(groupList);
        MY_LOG(INFO_LEVEL, "extracted " << ptr->size() << " elements");

        // filter only matching values
        for (const auto& [key, item] : *ptr) {
            bool matches = true;
            for (int i = 0; i < filters.size(); ++i) {
                auto valueId = filters[i]->getValueId();
                if (valueId == INVALID_ID) {
                    matches = false;
                    break;
                }

                if (item.groupValues[matchingIndexes[i]].valueId != valueId) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                (*map)[key] = item;
            }
        }
        return true;
    }

    // returns true if success
    // only modifies groupList if optimiztion was found
    bool tryOptimizedGroupQuery(GroupList* groupList) {
        if (tryFilterBreakdownIsCachedOptimization(groupList)) {
            return true;
        }

        if (tryNoFilterCachedOptimization(groupList)) {
            return true;
        }

        if (tryLookupOptimization(groupList)) {
            return true;
        }
        return false;
    }

    void aggregateIntoMap(GroupList* groupList) {
        if (tryOptimizedGroupQuery(groupList)) {
            return;
        }

        // otherwise fallback naive case
        FOR_EACH_ACCOUNT_ID(id) {
            TRY_GET_CONST_DATA(id, data);
            if (groupList->matches(id, data)) {
                groupList->updateMap(data, *map, ADD_ACCOUNT);
            }
        }
    }
};
