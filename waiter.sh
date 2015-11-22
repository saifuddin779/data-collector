#/bin/sh
while ps aux | grep [a]pt;
do
	echo "sleeping.."
	sleep 5
done
apt-get update
echo "everything done.."