import sys, os, random, string, time
from subprocess import call, Popen, PIPE, STDOUT
from itertools import cycle
from datetime import datetime as dt

import redis, sqlite3 as sql, skiff

from indeed.configs import configs

modes = {
			'test': {'token': 'b57edf366525324117fdcf42a1fe433327763ecae070c9ac01519ff4e5b0dab3', 'master': '104.131.30.114'},
			'prod': {'token': 'b6444ba62f087259dc2bc93396d004b57473074a814183ab4693aeb8fe77290b', 'master': '162.243.85.176'},
			}

droplets_db = '../data/droplets_db.db'
skillset = configs['skills_file']
r_master = redis.StrictRedis('localhost', '6379') 

class DigitalOceanManager(object):
	def __init__(self, country, mode):
		self.country = country
		self.mode = mode
		self.do_token = modes[self.mode]['token']
		self.master_node = modes[self.mode]['master']
		self.regions = cycle(['nyc1', 'nyc2', 'nyc3'])
		self.image ='ubuntu-14-04-x64'
		self.skiffer = skiff.rig(self.do_token)
		self.count_drops = 0
		self.backup = False

	def create_droplet(self, slug_size='512mb'):
		random_postfix = ''.join(random.choice(string.lowercase) for x in range(8))
		node_name = 'indeed-slave-%s-%s' % (self.country, random_postfix)
		droplet = self.skiffer.Droplet.create(
										{
											'name': node_name,
	                               			'region': self.regions.next(),
	                               			'image': 'ubuntu-14-04-x64',
	                               			'size':  slug_size,
	                               			'ssh_keys': self.skiffer.Key.all()
	                               		}
                               		)
		droplet.wait_till_done()
		droplet = droplet.refresh()
		return droplet

	def save_droplet(self, droplet):
		droplet_id = droplet.id
		droplet_name = droplet.name
		droplet_db = '%s.db' % droplet_name
		timestamp  = dt.fromtimestamp(time.time())
		for k in droplet.v4:
			ip_address = k.ip_address
		print ip_address
		con = sql.connect(droplets_db)
		cur = con.cursor()
		cur.execute('CREATE TABLE IF NOT EXISTS droplets_table (id INTEGER PRIMARY KEY AUTOINCREMENT, droplet_id VARCHAR, droplet_name VARCHAR, ip_address VARCHAR, db_name VARCHAR, time TIMESTAMP, count INT NOT NULL);');
		cur.execute("INSERT INTO droplets_table (droplet_id, droplet_name, ip_address, db_name, time, count) VALUES (?, ?, ?, ?, ?, ?);", (droplet_id, droplet_name, ip_address, droplet_db, timestamp, 0))
		con.commit()
		con.close()
		return

	def kick_start(self, droplet):
		print 'sleeping for 20 secs before %s gets ready..' % droplet.name
		time.sleep(20)
		get_latest_db_query = "select droplet_id, droplet_name, ip_address from droplets_table order by time desc limit 1;"
		con = sql.connect(droplets_db)
		cur = con.cursor()
		cur.execute(get_latest_db_query)
		for droplet_id, droplet_name, ip_address in cur:
			commands  = [
						'scp -o "StrictHostKeyChecking no" install.sh root@%s:install.sh' % ip_address, #--sends the install.sh
						'ssh -o "StrictHostKeyChecking no" -t root@%s bash install.sh %s %s' % (ip_address, self.country, self.master_node) #--runs the install.sh on slave
						]
			execute = call([commands[0]], shell=True)
			p = Popen(commands[1], shell=True, stdout=PIPE, stderr=STDOUT, bufsize=1, close_fds=True)
			print p, droplet.name
			#for command_ in commands:
			#	execute = subprocess.call([command_], shell=True)
		con.close()

	def destroy(self, droplet):
		droplet.destroy()
		print '%s destroyed..' % droplet.name
		return 

def get_skills_count(file_name):
	i = 0
	f = open('indeed/%s.json' % file_name, 'rb')
	for a in f:
		i += 1
	f.close()
	return i

def country_count(country):
	g = r_master.get(country)
	if not g:
		return 0
	else:
		return int(g)

if __name__ == '__main__':
	#if 'country' in sys.argv:
	country = sys.argv[1]
	mode = sys.argv[2]

	#--now read the content of the skills_file to get the count and compare with what is in the redis
	skills_count = get_skills_count(skillset)

	obj = DigitalOceanManager(country, mode)
	
	while skills_count != country_count(country):
		droplet = obj.create_droplet()
		obj.save_droplet(droplet)
		obj.kick_start(droplet)
		while not r_master.hget('droplets', droplet.name):
			print 'waiting for %s to finish...its working at %d' % (droplet.name, country_count(country))
			time.sleep(60)
		print 'destroying %s ..' % droplet.name
		obj.destroy(droplet)



