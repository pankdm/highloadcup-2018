#!/usr/bin/env python

import sys
import json
import requests
from collections import defaultdict
import time
import requests

## Usage: ./poster.py LIMIT DIR
LIMIT = 0
DIR = 'input-data/test_accounts_240119'


# 0 # 239 POST:/accounts/<id>/
# 1 # POST /accounts/11554/?query_id=5 HTTP/1.1
# 2 # Host: accounts.com
# 3 # User-Agent: Technolab/1.0 (Docker; CentOS) Highload/1.0
# 4 # Accept: */*
# 5 # Connection: close
# 6 # Content-Length: 31
# 7 # Content-Type: application/json
# 8
# 9 # {"email":"mahsemous@ymail.com"}

def keep(q):
    return True
    patterns = ["query_id=6063", "query_id=8470", "query_id=9281", "query_id=9305"]
    for p in patterns:
        if p in q:
            return True
    return False


# POST    /accounts/new/?query_id=0       201     {}

class Ammo:
    def __init__(self):
        self.query = None
        self.body = None

    def show(self):
        print 'Ammo'
        print 'query = ', self.query
        print 'body = ', self.body

    def do_post_request(self):
        url = "http://127.0.0.1:8081{}".format(self.query)
        r = requests.post(url, data=self.body)
        return r.status_code

class Answer:
    def __init__(self):
        self.code = None
        self.query = None

    def show(self):
        print 'Answer'
        print 'code = ', code
        print 'quert = ', query

def get_post_request_type(q):
    parts = q.split('/')
    type = parts[2]
    if type == 'likes' or type == 'new':
        return type
    return 'update'

class RequestStats:
    def __init__(self):
        self.num = 0
        self.total_time_ms = 0

    def get_average(self):
        if self.num == 0:
            return 0.0
        return float(self.total_time_ms) / self.num

class Poster:
    def __init__(self):
        self.stats_by_type = defaultdict(RequestStats)


    def read_answers(self, file_name):
        f = open(file_name, 'rt')
        answers = {}

        counter = 0
        while (True):
            line = f.readline()
            if line == '':
                return answers
            parts = line.split('\t')

            ans = Answer()
            ans.query = parts[1]
            ans.code = int(parts[2])

            answers[ans.query] = ans

            counter += 1
            if LIMIT and counter >= LIMIT:
                return answers

    def print_stats(self):
        types = list(self.stats_by_type.keys())
        # print types
        total_time = 0
        types.sort(key = lambda x : self.stats_by_type[x].total_time_ms, reverse=True)
        for type in types:
            stats = self.stats_by_type[type]
            print '  ==> {} ({}) --> avg {:.1f} ms, total = {:.1f} ms'.format(
                type, stats.num, stats.get_average(), stats.total_time_ms)
            total_time += stats.total_time_ms
        print 'total time = {:.1f} ms'.format(total_time)

    def read_ammos(self, file_name):
        f = open(file_name, 'rt')
        ammos = []

        counter = 0
        line = f.readline()
        while (True):
            batch = []
            while (True):
                line = f.readline()
                if line == '':
                    return ammos
                if 'POST:' in line:
                    break
                batch.append( line )
            # print batch[0]
            ammo = Ammo()
            ammo.query = batch[0].split(' ')[1]
            ammo.body = batch[8]
            ammos.append(ammo)

            counter += 1
            if LIMIT and counter >= LIMIT:
                return ammos

    def run(self, directory):
        print 'Using', directory, 'for POST requests', 'LIMIT = ', LIMIT
        ammos_file = directory + '/ammo/phase_2_post.ammo'
        answers_file = directory + '/answers/phase_2_post.answ'

        ammos = self.read_ammos(ammos_file)
        answers = self.read_answers(answers_file)

        print 'ammo = ', ammos_file, 'got', len(ammos)
        print 'answers = ', answers_file, 'got', len(answers)
        assert len(ammos) <= len(answers)

        self.run_posts(ammos, answers)

    def run_posts(self, ammos, answers):
        errors = 0
        error_by_request = defaultdict(list)
        ts = int(time.time())
        # f = open('perf-logs/perf-{}.txt'.format(ts), 'wt')

        for i in xrange(len(ammos)):
            ammo = ammos[i]

            if not keep(ammo.query):
                continue
            # if '/accounts/likes/?query_id=959' not in ammo.query:
            #     continue

            # if 'new' not in ammo.query:
            #     continue

            ans = answers.get(ammo.query, None)
            if ans is None:
                print 'Query "{}" not found'.format(ammo.query)
                continue

            if ammo.query != ans.query:
                print 'WARNING: Queries mismatch: ', ammo.query, ' vs ', ans.query
                continue

            start = time.time()
            print 'running', ammo.query, '-->',

            code = ammo.do_post_request()
            end = time.time()
            duration_ms = (end - start) * 1000.
            print code, '| {:.2f}'.format(duration_ms), 'ms | ',

            request_type = get_post_request_type(ammo.query)
            self.stats_by_type[request_type].num += 1
            self.stats_by_type[request_type].total_time_ms += duration_ms


            if code != ans.code:
                print 'ERROR'
                print 'Wrong code: {}, expected: {}'.format(code, ans.code)
                js = json.dumps(json.loads(ammo.body), indent=4)
                print js.encode("utf8")
                error_by_request[request_type].append(js)
                errors += 1
            else:
                print 'OK'

        self.print_stats()
        print 'total errors = ', errors
        for type, qq in error_by_request.items():
            print '  "{}" --> total errors = {}'.format(type, len(qq))
            # for q in qq:
            #     print ""
            #     print  q



poster = Poster()

if len(sys.argv) >= 2:
    LIMIT = int(sys.argv[1])

if len(sys.argv) >= 3:
    DIR = sys.argv[2]

poster.run(DIR)
