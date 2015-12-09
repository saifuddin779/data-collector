#/bin/sh
country="$1"
master="$2"

while ps aux | grep [a]pt;
do
	echo "sleeping.."
	sleep 5
done

echo y | apt-get update
echo y | apt-get install git
echo y | apt-get install gcc
echo y | apt-get install python-dev
echo y | apt-get install build-essential
echo y | apt-get install python-yaml
echo y | apt-get install python-lxml
echo y | apt-get install python-pip

#echo y | apt-get install autoconf libtool pkg-config python-opengl python-imaging python-pyrex python-pyside.qtopengl idle-python2.7 qt4-dev-tools qt4-designer libqtgui4 libqtcore4 libqt4-xml libqt4-test libqt4-script libqt4-network libqt4-dbus python-qt4 python-qt4-gl libgle3 python-dev
#echo y | apt-get install python-dateutil python-docutils python-feedparser python-gdata python-jinja2 python-ldap python-libxslt1 python-lxml python-mako python-mock python-openid python-psycopg2 python-psutil python-pybabel python-pychart python-pydot python-pyparsing python-reportlab python-simplejson python-tz python-unittest2 python-vatnumber python-vobject python-webdav python-werkzeug python-xlwt python-yaml python-zsi

git config --global user.name "saifuddin778"
git config --global user.email "saif.778@gmail.com"

echo y | apt-get install tmux
echo y | apt-get install htop
echo y | apt-get install sqlite3
#echo y | apt-get install nginx
echo y | apt-get install sshpass

#make directory for dataset
mkdir data/
mkdir data/resumes/
mkdir data/collect/

#download repo
git clone https://saifuddin779:saifuddin7791@github.com/saifuddin779/data-collector.git
echo "DONE CLONING REPO.."
pip install -r data-collector/requirements.txt
service rsyslog stop

#run the tmux session
tmux new -d -s scrap_session
# tmux send -t scrap_session.0 "cd data-collector/indeed/" ENTER
# tmux send -t scrap_session.0 "python db_access.py generate" ENTER
# tmux send -t scrap_session.0 "python parse_profiles.py $country $master" ENTER
