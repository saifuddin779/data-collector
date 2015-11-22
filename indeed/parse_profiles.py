import sys, os, requests, random, string, json, locale, gc, socket, subprocess
from time import time as tm, sleep as slp
from itertools import cycle

import redis
from pyquery import PyQuery as pq_

from cities import countries
from configs import configs
from db_access import db_insert_hash, get_distincts
from user_agents import user_agents

data_dir = '../../data/'
locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' ) 
skillset = configs['skills_file'] #--change this to change the filename

class indeed_resumes(object):
	def __init__(self, country_code, master):
		self.country_code = country_code
		self.master = master
		self.keywords = open(skillset+'.json', 'rb')
		#self.keywords = ['mongo', 'python', 'sql']
		self.init_url = 'http://www.indeed.com/resumes?q=%s&co='+self.country_code+'&start=%d&limit=%d'
		self.fixed_test_url = 'http://www.indeed.com/resumes?q=excel&co='+self.country_code
		self.url_ = 'http://www.indeed.com/resumes%s'
		self.user_agents_cycle = cycle(user_agents)
		self.r_master = redis.StrictRedis(host=self.master, port='6379')
		self.n_all = 0
	

	def init_redis(self):
		if not self.r_host.get('all_count'):
			self.r_host.set('all_count', 0)
		return
	

	def resource_collection(self, keyword, sort, rest_kewords=False):
		start_time = tm()
		print keyword
		n_profiles = {}
		keyword = '%s' % keyword.replace('/', ' ')
		keyword = keyword.strip('\n')
		
		#init_url = self.init_url % (keyword.replace(' ', '+'), self.location.replace(' ', '+'), 0, 50)
		init_url = self.init_url % (keyword.replace(' ', '+'), 0, 50)
		filtering_urls = self.get_filter_urls(init_url)

		if not filtering_urls:
			check = self.get_filter_urls(self.fixed_test_url)
			if not check:
				#--do a plain check with a regular, simple keyword like ms excel
				#--if that keyword also results in nothing, then you are blocked.
				#--simply send the database, update the droplet name to master, and exit and master will destroy you.
				print 'THE CONDITION IS HERE..'
				self.send_to_master()
				self.r_master.hset('droplets', socket.gethostname(),  True)
				print 'sent db to master...terminating..'
				sys.exit()
		
		for route in filtering_urls:
			url_ = self.url_ % pq_(route).children('a').attr('href')
			for i in range(15):
				if i == 0:
					beg = i
					end = i+100
				else:
					beg = end
					end = end+100
				postfix = '&start=%d&limit=%d&radius=100&%s&co=%s' % (beg, end, sort, self.country_code)	
				data = self.get_resource(url_+postfix)
				if not data:
					check = self.get_resource(self.fixed_test_url)
					if not check:
						break
				
				for each in data:
					item = pq_(each)
					unique_id = item.attr('id')
					city_ = item('.location').text()
					n_profiles[unique_id] = city_
			try:
				db_insert_hash(n_profiles, self.country_code)
			except:
				print 'db locked..will wait for few secs'
				slp(20)
				db_insert_hash(n_profiles, self.country_code)
			print 'inserted %d records to db.. %s' % (len(n_profiles), keyword)	
			n_profiles = {}
			gc.collect()
		gc.collect()
		current_time = tm()
		print 'current time passed..%d' % int(current_time - begin_time)
		return
	

	def get_filter_urls(self, init_url):
		filtering_urls = []
		resp = None
		while not resp:
			try:
				user_agent = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))
				resp = requests.get(init_url, headers = {'user_agent': user_agent})
			except Exception, e:
				print str(e)
				slp(30)
				pass
		if resp.status_code == 200:
			filtering_urls = pq_(resp.text)
			filtering_urls = filtering_urls('.refinement')
			return filtering_urls
		else:
			return filtering_urls
	

	def get_resource(self, url_):
		data = []
		resp = None
		while not resp:
			try:
				user_agent = self.user_agents_cycle.next()
				resp = requests.get(url_, headers = {'user_agent': user_agent})
			except Exception, e:
				print str(e)
				slp(30)
				pass
		if resp.status_code == 200:
			data = pq_(resp.text)
			data = data('#results').children()
			return data
		else:
			return data

	
	def begin(self):
		sorts = ['sort=date', '']
		keywords_done_idx = self.r_master.get(self.country_code) #--this over here should talk to master's redis
		print 'starting from %s' % str(keywords_done_idx)
		if not keywords_done_idx:
			keywords_done_idx = -1
		else:
			keywords_done_idx = int(keywords_done_idx)
		for i, keyword in enumerate(self.keywords):
			keyword = keyword.replace('\n', '')
			if i <= keywords_done_idx:
				continue
			else:
				for sort in sorts:
					self.resource_collection(keyword, sort)
				self.r_master.set(self.country_code, i)
		self.send_to_master()
		self.r_master.hset('droplets', socket.gethostname(),  True)
		print 'sent db to master...terminating..'
		self.keywords.close()
		sys.exit()
		return


	def send_to_master(self):
		host_name = socket.gethostname()
		command_ = "sshpass -p indeed-master-01 scp -o 'StrictHostKeyChecking no' %s%s.db root@%s:data/droplets_dbs/%s.db" % (data_dir, host_name, self.master, host_name)
		print command_
		execute = subprocess.call([command_], shell=True)
		return


if __name__ == '__main__':
	countries_list = ['US', 'IN', 'GB', 'AU', 'CA']
	begin_time = tm()
	country_code = sys.argv[1]
	master = sys.argv[2]
	obj = indeed_resumes(country_code, master)
	obj.begin()
