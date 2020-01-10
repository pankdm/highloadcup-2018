#pragma once

#include "Types.h"

struct IdIterator {
    virtual ~IdIterator() = default;

    virtual bool valid() = 0;

    // it is guranteed that next() will skip repeating values
    virtual void next() = 0;

    virtual int32_t size() = 0;
    virtual AccountId getId() = 0;
};

struct IdListIterator : public IdIterator {
    const AccountIdList* list{nullptr};
    int current{0};
    int end{0};

    // empty iterator
    IdListIterator() = default;

    explicit IdListIterator(const AccountIdList& list_) : list(&list_), end(list_.size()) {}

    void next() override { ++current; }

    bool valid() override { return current < end; }

    int32_t size() override {
        if (list) {
            return list->size();
        } else {
            return 0;
        }
    }

    AccountId getId() override { return (*list)[current]; }
};

struct LikeEdgeIterator : public IdIterator {
    const EdgeList* list{nullptr};
    int current{0};
    int end{0};

    // empty iterator
    LikeEdgeIterator() = default;

    explicit LikeEdgeIterator(const EdgeList& list_) : list(&list_), end(list_.size()) {}

    void next() override {
        if (!list) {
            // if iterator is empty do nothing
            return;
        }
        // std::cout << "::next(), current = " << current;
        // skip repeating ids
        auto lastId = getId();
        ++current;
        while (valid() && (getId() == lastId)) {
            ++current;
        }
    }

    bool valid() override { return current < end; }

    int32_t size() override {
        if (list) {
            return list->size();
        } else {
            return 0;
        }
    }

    AccountId getId() override { return (*list)[current].accountId; }
};

struct IntersectionIdIterator : public IdIterator {
    // assumption: a and b should both go in reverse order
    std::unique_ptr<IdIterator> a;
    std::unique_ptr<IdIterator> b;
    AccountId value{INVALID_ID};

    IntersectionIdIterator(std::unique_ptr<IdIterator> a_, std::unique_ptr<IdIterator> b_)
        : a(std::move(a_)), b(std::move(b_)) {}

    bool valid() override {
        iterateUntilMatch();
        return a->valid() && b->valid();
    }
    void next() override {
        // TODO: remove this debug code
        if (a->valid() && b->valid()) {
            MY_ASSERT(a->getId() == b->getId());
            MY_ASSERT(value == a->getId());
        }

        while (a->valid() && (a->getId() == value)) {
            a->next();
        }
        while (b->valid() && (b->getId() == value)) {
            b->next();
        }
        iterateUntilMatch();
    }

    int32_t size() override { return std::min(a->size(), b->size()); }

    AccountId getId() override { return value; }

    // or until one of them becomes invalid
    void iterateUntilMatch() {
        while (true) {
            if (!a->valid()) return;
            if (!b->valid()) return;
            auto va = a->getId();
            auto vb = b->getId();
            if (va == vb) {
                value = va;
                return;
            }
            if (va > vb) {
                a->next();
                continue;
            } else {
                b->next();
                continue;
            }
        }
    }
};

#define CREATE_EMPTY_ITERATOR() std::make_unique<IdListIterator>()
#define RETURN_ITERATOR_FROM_MAP(map, value)           \
    MY_ASSERT(index);                                  \
    auto ptr = stl::mapGetPtr(index->map, value);      \
    if (ptr) {                                         \
        return std::make_unique<IdListIterator>(*ptr); \
    } else {                                           \
        return CREATE_EMPTY_ITERATOR();                \
    }
