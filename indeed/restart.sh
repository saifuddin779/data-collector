#/bin/sh
index="$1"

pkill -9 python
sleep 3
python parse_profiles.py US prod $index

