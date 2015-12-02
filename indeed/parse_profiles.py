import sys, os, requests, random, string, json, locale, gc, socket, subprocess
from time import time as tm, sleep as slp
from itertools import cycle

import redis
from pyquery import PyQuery as pq_

from cities import countries
from configs import configs
from parse_profile_details import indeed_resumes_details
from db_access import db_insert_hash, get_distincts, id_exists

from user_agents import user_agents



data_dir = '../../data/'
locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' ) 
skillset = configs['skills_file'] #--change this to change the filename

nodes_index = {
				'indeed-master-01': {'index': 1, 'ip': '104.131.30.114', 'next': 'indeed-master-02'},
				'indeed-master-02': {'index': 2, 'ip': '162.243.8.180', 'next': 'indeed-master-03'},
				'indeed-master-03': {'index': 3, 'ip': '159.203.126.194', 'next': 'indeed-master-01'}
			  }


class indeed_resumes(object):
	def __init__(self, country_code, master, index):
		self.country_code = country_code
		self.master = master
		self.index = int(index)
		self.keywords = open(skillset+'.json', 'rb')
		self.init_url = 'http://www.indeed.com/resumes?q=%s&co='+self.country_code+'&start=%d&limit=%d'
		self.fixed_test_url = 'http://www.indeed.com/resumes?q=excel&co='+self.country_code
		self.url_ = 'http://www.indeed.com/resumes%s'
		self.user_agents_cycle = cycle(user_agents)
		self.max_recursion_depth = 7
		self.time_all = []

	def init_redis(self):
		if not self.r_host.get('all_count'):
			self.r_host.set('all_count', 0)
		return

	def save_to_disk(self, data, unique_id):
		if not id_exists(unique_id):
			directory = '../../data/resumes/%s' % unique_id[0:2]
			if not os.path.exists(directory):
				os.makedirs(directory)
			filename = '%s/%s.json' % (directory, unique_id)
			f = open(filename, 'wb')
			f.write(json.dumps(data))
			f.close()
		return

	def resource_collection(self, keyword_index, keyword, sort, rest_kewords=False):
		start_time = tm()
		n_all = 0
		n_profiles = {}
		keyword = '%s' % keyword.replace('/', ' ')
		keyword = keyword.strip('\n')
		init_url = self.init_url % (keyword.replace(' ', '+'), 0, 50)
		
		filtering_urls, result_count  = self.get_filter_urls(init_url, 0)
		
		if result_count >= 1000:
			counter = 10
		else:
			counter = int(max(float(result_count)/100, 1))
		
		for route in filtering_urls:
			url_ = self.url_ % pq_(route).children('a').attr('href')
			for i in range(counter):
				if i == 0:
					beg = i
					end = i+100
				else:
					beg = end
					end = end+100
				postfix = '&start=%d&limit=%d&radius=100&%s&co=%s' % (beg, end, sort, self.country_code)	
				print url_+postfix	
				data = self.get_resource(url_+postfix, 0)

				for each in data:
					item = pq_(each)
					unique_id = item.attr('id')
					city_ = item('.location').text()
					n_profiles[unique_id] = city_
					profile_data = indeed_resumes_details(unique_id).resource_collection()
					self.save_to_disk(profile_data, unique_id)
					n_all += 1

			db_success = False
			while not db_success:
				try:
					db_insert_hash(n_profiles, self.country_code)
					db_success = True
				except:
					print 'db locked..will wait for 5 secs and try again..'
					slp(5)
					pass
			print 'inserted %d records to db.. %s, %d' % (len(n_profiles), keyword, keyword_index)	
			n_profiles = {}
			slp(0) #--sleeping for 2 secs for every filter for not making calls too fast and get blocked quickly
			gc.collect()
		gc.collect()
		current_time = tm()
		self.time_all.append((keyword, n_all, current_time - start_time))
		print 'current time passed..%d secs for one round of %s (%d)' % (int(current_time - begin_time), keyword, keyword_index)
		return
	

	def get_filter_urls(self, init_url, counter):
		if counter >= self.max_recursion_depth:
			print 'max recursion depth achieved in the get_filter_urls'
			#slp(300)
			return ([], 0)
		
		filtering_urls = []
		resp = None
		while not resp:
			try:
				user_agent = self.user_agents_cycle.next()
				resp = requests.get(init_url, headers = {'user_agent': user_agent})
			except Exception, e:
				print str(e), '###'
				slp(10)
				pass
		if resp.status_code == 200 or len(self.get_static_resource(self.fixed_test_url)):
			filtering_urls = pq_(resp.text)
			count =  filtering_urls('#search_header #rezsearch #search_table #result_count').text().split(' ')[0].replace(',', '')
			filtering_urls = filtering_urls('.refinement')
			if count.isdigit():
				count = int(count)
			else:
				count = 0
			return (filtering_urls, count)
		else:
			slp(1)
			return self.get_filter_urls(init_url, counter+1)

	def get_resource(self, url_, counter):
		if counter >= self.max_recursion_depth:
			print 'max recursion depth achieved in the get_resource'
			#slp(300)
			return []
		data = []
		resp = None
		while not resp:
			try:
				user_agent = self.user_agents_cycle.next()
				resp = requests.get(url_, headers = {'user_agent': user_agent})
			except Exception, e:
				print str(e), '@@@'
				slp(10)
				pass
		if resp.status_code == 200 or len(self.get_static_resource(self.fixed_test_url)):
			data = pq_(resp.text)
			data = data('#results').children()
			return data
		else:
			slp(1)
			return self.get_resource(url_, counter+1)

	def get_static_resource(self, url):
		data = []
		resp = None
		try:
			while not resp:
				try:
					user_agent = self.user_agents_cycle.next()
					resp = requests.get(url, headers = {'user_agent': user_agent})
				except Exception, e:
					print str(e), '!!!'
					slp(5)
					pass
			if resp.status_code == 200:
				data = pq_(resp.text)
				data = data('#results').children()
				return data
			else:
				return data
		except RuntimeError:
			return data

	
	def begin(self):
		job_start_time = tm()
		sorts = ['sort=date', '']
		keywords_done_idx = self.index
		#keywords_done_idx = self.r_master.get(self.country_code) #--this over here should talk to master's redis
		print 'starting from %s' % str(keywords_done_idx)
		if not keywords_done_idx:
			keywords_done_idx = -1
		else:
			keywords_done_idx = int(keywords_done_idx)
		
		for i, keyword in enumerate(self.keywords):
			#keyword = keyword.replace('\n', '')
			keyword = "'"
			if i <= keywords_done_idx:
				continue
			else:
				print 'now working on..%d in begin..' % i 
				for sort in sorts:
					self.resource_collection(i, keyword, sort)
				#--checking the block
				if sum(map(lambda p: p[1], self.time_all[-2:])) == 0 and len(self.time_all) > 2:
					check = self.get_static_resource(self.fixed_test_url)
					if not len(check):
						print 'putting to sleep for 10 mins because last 4 keywords went nill and check indicated block..'
						print 'currently worked at .. %d' % i
						slp(1200)
				#--switching to the sibling node every hour
				time_right_now = tm()
				if (time_right_now - job_start_time) >= 100:
					host_name = socket.gethostname() #--get current hostname
					
					sibling_name = nodes_index[host_name]['next'] #--get its sibling name
					sibling_ip = nodes_index[sibling_name]['ip'] #--get the siblings ip
					sibling_url = 'http://%s:5000/begin/' % sibling_ip
					payload = {'index': i, 'country_code': self.country_code} #--index of the keyword in process
					r = requests.get(sibling_url, params=payload) #--ask the sibling to begin collection from the given index
					if r.status_code == 200:
						print r.text
						print '%s closing at %d..will begin soon..' % (host_name, i)
					self.keywords.close()
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
	index = sys.argv[3]
	obj = indeed_resumes(country_code, master, index, next_node_index)
	obj.begin()
