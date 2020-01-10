#!/bin/bash -x

echo 'installing jemalloc'
cd /third_party/jemalloc
./autogen.sh > /dev/null
make dist > /dev/null
make > /dev/null
make install > /dev/null
cd /src/my_webserver

# echo 'running make'
PRODUCTION=1 make

# echo 'preparing data'
# ls -l /tmp/data/
cat /tmp/data/options.txt

mkdir -p ../../data

cp /tmp/data/options.txt ../../data/

unzip -o /tmp/data/data.zip -d ../../data/ > /dev/null

# mkdir -p /tmp/zzz
# unzip -o /tmp/data.zip -d /tmp/zzz/ >/dev/null
# mv /tmp/zzz/data/* ../../data/

# echo 'starting service'
./build/server 80 ../../data/
