
import sys
from collections import defaultdict


mode = sys.argv[1]


def count_query_item_freq(mode):
    stats = defaultdict(int)
    for l in sys.stdin:
        parts = l.split()
        # print parts
        q = parts[6]
        code = int(parts[7])
        if code != 200:
            continue
        kv = q.split('?')[1].split('&')
        for item in kv:
            if mode == "k":
                key = item.split('=')[0]
            else:
                key = item
            stats[key] += 1

    for key, cnt in stats.items():
        print cnt, '\t', key

def analyze_group_breakdowns(mode):
    stats = defaultdict(int)
    for l in sys.stdin:
        parts = l.split()
        # print parts
        q = parts[6]
        code = int(parts[7])
        if code != 200:
            continue
        kv = q.split('?')[1].split('&')
        params = {}
        filter = []
        group = []
        group_fields = ["sex", "status", "interests", "city", "country"]
        keys = None
        for item in kv:
            key, value = item.split('=')
            if key in ["limit", "query_id", "order"]:
                continue

            if key == "keys":
                keys = value.replace("%2C", ",")
            else:
                params[key] = value
                if key in group_fields:
                    group.append(key)
                else:
                    filter.append(key)

        filter.sort()
        all_group = list(set(keys.split(',') + group))
        all_group_joined = ",".join(all_group)

        # cache_key = "{} # {} # {}".format(len(all_group) + len(filter), all_group_joined, ",".join(filter))
        cache_key = "{} # {} # {}".format(keys, ",".join(group), ",".join(filter))
        stats[cache_key] += 1

    for key, cnt in stats.items():
        print cnt, '\t', key


if mode == "k" or mode == "v":
    count_query_item_freq(mode)
else:
    analyze_group_breakdowns(mode)



# FILTER
# $ show-perf-all ../queries-all.txt  |grep "filter" | python src/py/stats.py  | sort -n
# 123 	phone_code
# 126 	phone_null
# 162 	fname_any
# 172 	sname_null
# 173 	sname_starts
# 187 	fname_null
# 288 	email_domain
# 339 	email_gt
# 364 	email_lt
# 396 	premium_null
# 407 	premium_now
# 490 	city_any
# + 507 	birth_year
# + 511 	city_null
# 534 	birth_gt
# 534 	birth_lt
# + 553 	city_eq
# 746 	likes_contains
# 971 	interests_any
# 1057 	interests_contains
# 1125 	status_neq
# + 1151 	status_eq
# + 1236 	country_null
# + 1297 	country_eq
# + 4642 	sex_eq
# 7662 	limit
# 7662 	query_id

# with specific values:
# 47 	country_eq=%D0%A0%D0%BE%D1%81%D0%B0%D1%82%D1%80%D0%B8%D1%81
# 48 	country_eq=%D0%A0%D1%83%D0%BC%D0%BB%D1%8F%D0%BD%D0%B4%D0%B8%D1%8F
# 50 	country_eq=%D0%9C%D0%B0%D0%BB%D0%BB%D1%8F%D0%BD%D0%B4%D0%B8%D1%8F
# 56 	phone_null=1
# 70 	phone_null=0
# 172 	sname_null=0
# 187 	fname_null=0
# 187 	premium_null=1
# 209 	premium_null=0
# 252 	city_null=1
# 259 	city_null=0
# 353 	status_neq=%D0%B7%D0%B0%D0%BD%D1%8F%D1%82%D1%8B
# 369 	status_neq=%D0%B2%D1%81%D1%91+%D1%81%D0%BB%D0%BE%D0%B6%D0%BD%D0%BE
# 377 	status_eq=%D0%B7%D0%B0%D0%BD%D1%8F%D1%82%D1%8B
# 378 	status_eq=%D0%B2%D1%81%D1%91+%D1%81%D0%BB%D0%BE%D0%B6%D0%BD%D0%BE
# 396 	status_eq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B
# 403 	status_neq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B
# 407 	premium_now=1
# 609 	country_null=1
# 627 	country_null=0
# 2308 	sex_eq=f
# 2334 	sex_eq=m


# GROUP
# show-perf-all ../queries-all.txt  |grep "group" | python src/py/stats.py k | sort -n
# 277 	city
# 303 	country
# 369 	sex
# 376 	interests
# 530 	status
# 582 	likes
# 873 	birth
# 905 	joined
# 2970 	keys
# 2970 	limit
# 2970 	order
# 2970 	query_id
