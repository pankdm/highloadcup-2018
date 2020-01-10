#pragma once

#include "Base.h"
#include "Util.h"

template <class TId, class TValue>
class IdValueMap {
   public:
    TId getOrCreateId(const TValue& value) {
        auto ptr = stl::mapGetPtr(_valueToIdMap, value);
        if (ptr) {
            return *ptr;
        } else {
            TId id = _idToValueMap.size();
            MY_ASSERT(id != -1);
            _idToValueMap.push_back(value);
            _valueToIdMap[value] = id;
            return id;
        }
    }

    TId getId(const TValue& value) const {
        auto ptr = stl::mapGetPtr(_valueToIdMap, value);
        if (ptr) {
            return *ptr;
        } else {
            return -1;
        }
    };

    const TValue& getValue(TId id) const {
        MY_ASSERT(id < _idToValueMap.size());
        return _idToValueMap[id];
    };

    size_t size() const { return _idToValueMap.size(); }

   private:
    std::unordered_map<TValue, TId> _valueToIdMap;
    std::vector<TValue> _idToValueMap;
};
