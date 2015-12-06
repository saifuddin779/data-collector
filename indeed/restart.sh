#/bin/sh
index="$1"

pkill -9 python
pkill -9 redis
killall python
/etc/init.d/tor stop
/etc/init.d/privoxy stop
sleep 10
/etc/init.d/tor start
/etc/init.d/privoxy start

python parse_profiles.py US prod $index