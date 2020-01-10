import sys
import json

# example usage:
# python src/py/pretty_print.py input-data/test_accounts_291218/answers/phase_1_get.answ > answers1.txt
file = sys.argv[1]
f = open(file)

for l in f.xreadlines():
    parts = l.strip('\n').split('\t')
    if (len(parts) < 4):
        print l
        continue
    type = parts[0]
    q = parts[1]
    code = parts[2]
    js_str = " ".join(parts[3:])
    js = json.loads(js_str)
    print type, q, code
    print json.dumps(js, indent=4, ensure_ascii=False).encode('utf8')
