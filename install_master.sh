echo y | apt-get update
echo y | apt-get install git
echo y | apt-get install gcc
echo y | apt-get install python-dev
echo y | apt-get install build-essential
echo y | apt-get install autoconf libtool pkg-config python-opengl python-imaging python-pyrex python-pyside.qtopengl idle-python2.7 qt4-dev-tools qt4-designer libqtgui4 libqtcore4 libqt4-xml libqt4-test libqt4-script libqt4-network libqt4-dbus python-qt4 python-qt4-gl libgle3 python-dev
echo y | apt-get install python-dateutil python-docutils python-feedparser python-gdata python-jinja2 python-ldap python-libxslt1 python-lxml python-mako python-mock python-openid python-psycopg2 python-psutil python-pybabel python-pychart python-pydot python-pyparsing python-reportlab python-simplejson python-tz python-unittest2 python-vatnumber python-vobject python-webdav python-werkzeug python-xlwt python-yaml python-zsi
echo y | apt-get install python-pip

git config --global user.name "saifuddin778"
git config --global user.email "saif.778@gmail.com"

echo y | apt-get install tmux
echo y | apt-get install htop
echo y | apt-get install sqlite3
echo y | apt-get install nginx
echo y | apt-get install sshpass

redis installation on the local machine section
echo y | apt-get install tcl8.5
wget http://download.redis.io/releases/redis-stable.tar.gz
tar xzf redis-stable.tar.gz
cd redis-stable
make
make install
make test
cd utils
echo y | ./install_server.sh
service redis_6379 start
cd ../../

#make directory for dataset
mkdir data/
mkdir data/droplets_db/

#download repo
git clone https://saifuddin779:saifuddin7791@github.com/saifuddin779/data-collector.git
echo "DONE CLONING REPO.."
pip install -r data-collector/requirements.txt
service rsyslog stop