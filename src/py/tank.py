#!/usr/bin/env python

import sys
import json
import requests
from collections import defaultdict
import time

COMPARE_RESULTS = True

MAX_RESPONSE_SIZE = 0


# example usage:
# cat input-data/elim_accounts_261218/answers/phase_1_get.answ |grep "/group" | head -n 100 |python src/py/tank.py
# cat input-data/elim_accounts_261218/answers/phase_1_get.answ | head -n 1000 | python src/py/tank.py
# cat input-data/elim_accounts_261218/answers/phase_1_get.answ | head -n 1000 | python src/py/tank.py
#
# cat input-data/test_accounts_240119/answers/phase_1_get.answ |python src/py/tank.py

# cat input-data/test_accounts_291218/answers/phase_1_get.answ |grep "/group" | head -n 100 |python src/py/tank.py
# cat input-data/test_accounts_220119/answers/phase_1_get.answ |python src/py/tank.py
#
# cat input-data/test_accounts_240119/answers/phase_3_get.answ |python src/py/tank.py


def do_post_request(query, body):
    url = "http://127.0.0.1:8081{}".format(query)
    r = requests.post(url, data=body)

    print(type(r))
    print(r.status_code)
    print(r.headers)
    print(r.headers['content-type'])


def do_request(query):
    global MAX_RESPONSE_SIZE
    url = "http://127.0.0.1:8081{}".format(query)
    r = requests.get(url)
    # print(type(r))
    # print(r.status_code)
    # print(r.headers)
    # print(r.headers['content-type'])
    js_str = r.text

    current_size = len(js_str)
    if  current_size > MAX_RESPONSE_SIZE:
        MAX_RESPONSE_SIZE = current_size
        print 'MAX_RESPONSE_SIZE = ', MAX_RESPONSE_SIZE

    if js_str == "":
        js = None
    else:
        js = json.loads(js_str)
    return r.status_code, js

def compare_accounts(index, item_got, item_expected):
    if (item_got['id'] != item_expected['id']):
        print '{}: wrong item id, got: {}, expected: {}'.format(
            index, item_got['id'], item_expected['id'])
        return False
    # TODO: compare field values
    # for k, v in item_expected.items():
    #     v_got = item_got.get(k, None)
    #     if (v_got != v):
    #         print '{}: field "{}" is different, got: {}, expected: {}'.format(
    #             index, k, v_got.encode('utf8'), v.encode('utf8'))
    #         return False
    return True

def compare_groups(index, group_got, group_expected):
    if group_got['count'] == 0:
        print 'Got 0 items for {} !!'.format(group_got)

    if (group_got['count'] != group_expected['count']):
        print '{}: wrong count, got: {}, expected: {}'.format(
            index, group_got['count'], group_expected['count'])
        return False
    # TODO: compare field values
    return True


def compare_data(data_got, data_expected, f):
    # print json.dumps(data_got, indent=4)
    # print json.dumps(data_expected, indent=4)
    ok = True
    if len(data_got) != len(data_expected):
        print 'Wrong response size: got: {}, expected: {}'.format(len(data_got), len(data_expected))
        ok = False
    index = 0
    while (index < len(data_got) and index < len(data_expected)):
        item_got = data_got[index]
        item_expected = data_expected[index]
        if (f(index, item_got, item_expected) == False):
            print 'GOT: ', json.dumps(item_got, indent = 4)
            print 'EXPECTED: ', json.dumps(item_expected, indent = 4)
            return False
        index += 1
    if index < len(data_got):
        print "GOT Extra: ", json.dumps(data_got[index], indent = 4)
    if index < len(data_expected):
        print "EXPECTED Extra: ", json.dumps(data_expected[index], indent = 4)
    return ok


def compare_results(js_got, js_expected):
    if "accounts" in js_expected:
        data_got = js_got["accounts"]
        data_expected = js_expected["accounts"]
        return compare_data(data_got, data_expected, compare_accounts)
    if "groups" in js_expected:
        data_got = js_got["groups"]
        data_expected = js_expected["groups"]
        return compare_data(data_got, data_expected, compare_groups)
    return True

def get_request_type(q):
    parts = q.split('/')
    if parts[2] == "filter" or parts[2] == "group":
        return parts[2]
    return parts[3]


class RequestStats:
    def __init__(self):
        self.num = 0
        self.total_time_ms = 0

    def get_average(self):
        if self.num == 0:
            return 0.0
        return float(self.total_time_ms) / self.num

class Tank:
    def __init__(self):
        self.stats_by_type = defaultdict(RequestStats)

    def print_stats(self, errors):
        types = list(self.stats_by_type.keys())
        # print types
        total_time = 0
        types.sort(key = lambda x : self.stats_by_type[x].total_time_ms, reverse=True)
        for type in types:
            stats = self.stats_by_type[type]
            print '  ==> {} ({}) --> avg {:.1f} ms, total = {:.1f}'.format(
                type, stats.num, stats.get_average(), stats.total_time_ms)
            total_time += stats.total_time_ms
        print 'total time = {:.1f} ms'.format(total_time)
        print 'total errors = ', errors


    def benchmark(self, query, times):
        for counter in xrange(times):
            start = time.time()
            code_got, js_got = do_request(query)
            end = time.time()
            duration_ms = (end - start) * 1000.

            request_type = get_request_type(query)
            self.stats_by_type[request_type].num += 1
            self.stats_by_type[request_type].total_time_ms += duration_ms

            msg = "{} | {:.1f} ms | {}".format(counter, duration_ms, query)
            print msg
        self.print_stats(None)

    def run(self):
        self.counter = 0
        errors = None
        if COMPARE_RESULTS:
            errors = 0

        error_by_request = defaultdict(list)
        ts = int(time.time())
        f = open('perf-logs/perf-{}.txt'.format(ts), 'wt')

        for l in sys.stdin:
            parts = l.strip('\n').split('\t')
            # if (len(parts) < 4):
            #     # print l
            #     continue
            type = parts[0]
            q = parts[1]
            code_expected = int(parts[2])

            # look only at 200s for now
            if (code_expected != 200):
                continue

            js_str = " ".join(parts[3:])
            if js_str == "":
                js_expected = None
            else:
                js_expected = json.loads(js_str)
            self.counter += 1

            start = time.time()
            # print 'doing ', q

            code_got, js_got = do_request(q)
            end = time.time()
            duration_ms = (end - start) * 1000.

            msg = "{} | {:.1f} ms | {} {} {}".format(self.counter, duration_ms, type, q, code_expected)
            # print msg
            print >> f, msg

            request_type = get_request_type(q)
            self.stats_by_type[request_type].num += 1
            self.stats_by_type[request_type].total_time_ms += duration_ms
            if (self.counter % 300 == 0):
                print ""
                print self.counter, 'requests'
                self.print_stats(errors)

            if code_got != code_expected:
                print msg
                print ("Wrong response code: {}, expected: {}".format(code_got, code_expected))
                continue
            # don't compare non 200 responses
            if code_expected != 200:
                # print 'OK'
                continue
            if COMPARE_RESULTS and (compare_results(js_got, js_expected) == False):
                print msg
                errors += 1
                error_by_request[request_type].append(q)
                # break
            # print 'OK'
            # print json.dumps(js, indent=4)

        f.close()

        print ""
        print '==> finished ', self.counter, ' requests'
        self.print_stats(0)
        print 'total errors = ', errors
        for type, qq in error_by_request.items():
            print '  "{}" --> total errors = {}'.format(type, len(qq))
            for q in qq:
                print "     ", q



tank = Tank()

if len(sys.argv) > 1:
    query = sys.argv[1]
    times = int(sys.argv[2])
    tank.benchmark(query, times)
else:
    tank.run()
