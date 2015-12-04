import sys, os, requests, random, string, json, locale, gc, socket, subprocess
#import grequests
from data_getter import get_data
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

	def save_to_disk(self, data):
		if data:
			try:
				directory = '../../data/resumes/%s' % data['unique_id'][0:2]
			except:
				return
			if not os.path.exists(directory):
				os.makedirs(directory)
			filename = '%s/%s.json' % (directory, data['unique_id'])
			f = open(filename, 'wb')
			f.write(json.dumps(data))
			f.close()
		return

	def chunk_it(self, seq, num):
		avg = len(seq) / float(num)
	  	out = []
	  	last = 0.0

	  	while last < len(seq):
	  		out.append(seq[int(last):int(last + avg)])
	  		last += avg
	  	return out

	def resource_collection(self, keyword_index, keyword, sort, rest_kewords=False):
		start_time = tm()
		n_profiles = {}
		final_all = 0
		keyword = '%s' % keyword.replace('/', ' ')
		keyword = keyword.strip('\n')
		init_url = self.init_url % (keyword.replace(' ', '+'), 0, 50)
		#filtering_urls, result_count  = self.get_filter_urls(init_url, 0)
		filtering_urls, result_count = self.get_filter_urls_px(init_url, 0)
		if result_count < 500:
			return
		
		for route in filtering_urls:
			n_all = 0
			url_ = self.url_ % pq_(route).children('a').attr('href')
			count = pq_(route).children('span').text().replace('+', '')
			if count.isdigit():
				count = int(count)
			else:
				count = 0

			if count >= 1000:
				counter = 10
			else:
				counter = int(max(float(count)/100, 1))

			routes_container = []
			for i in range(counter):
				if i == 0:
					beg = i
					end = i+100
				else:
					beg = end
					end = end+100
				postfix = '&start=%d&limit=%d&radius=100&%s&co=%s' % (beg, end, sort, self.country_code)
				routes_container.append(url_+postfix)

			t_res1 = tm()

			# data = []
			# routes_container = self.chunk_it(routes_container, 3)
			# routes_container = filter(lambda p: len(p), routes_container)
			# for routes_chunk in routes_container:
			# 	data.append(self.get_resource(routes_chunk, 0))
			
			data =[]
			for i_ in routes_container:
				#data.append(self.get_resource2(i_, 0))
				data.append(self.get_resource_px(i_, 0))
				slp(2)
			t_res2 = tm()
			
			print 'data is here in %f secs..--> %d' % (float(t_res2 - t_res1), len(data))

			profile_set = []
			for data_ in data:
				for id_set in data_:
					for unique_id in id_set:
						profile_set.append(unique_id)
						n_all += 1
			print 'the set is .. %d ids ..' % len(profile_set)
			slp(10)
			profile_set = filter(lambda n: n  != None, profile_set)
			t_prf1 = tm()
			profile_data = indeed_resumes_details(profile_set).resource_collection()
			for profile in profile_data:
				self.save_to_disk(profile)
			t_prf2 = tm()
			print 'profiles saved in %f secs.. --> %d' % (float(t_prf2 - t_prf1), len(profile_data))
			print 'inserted %d records to db.. %s, %d' % (n_all, keyword, keyword_index)
			n_profiles = {}
			slp(10) #--sleeping for 10 secs for every filter for not making calls too fast and get blocked quickly
			final_all += n_all
			gc.collect()

		current_time = tm()
		self.time_all.append((keyword, final_all, current_time - start_time))
		print 'current time passed..%d secs for one round of %s (%d)' % (int(current_time - begin_time), keyword, keyword_index)
		print 'total records collected for %s (%d) --> %d' % (keyword, keyword_index, final_all)
		return
	
	def get_filter_urls_px(self, init_url, counter):
		"""NEW -- PROXIED WAY OF GETTING DATA"""
		filtering_data = get_data(init_url)
		filtering_data = pq_(filtering_data)

		count =  filtering_data('#search_header #rezsearch #search_table #result_count').text().split(' ')[0].replace(',', '')
		filtering_urls = filtering_data('.refinement')

		if count.isdigit():
			count = int(count)
		else:
			count = 0
		return (filtering_urls, count)


	def get_filter_urls(self, init_url, counter):
		if counter >= self.max_recursion_depth:
			print 'max recursion depth achieved in the get_filter_urls'
			#slp(300)
			return ([], 0)
		
		filtering_urls = []
		try:
			user_agent = self.user_agents_cycle.next()
			resp = requests.get(init_url, headers = {'user_agent': user_agent})
		except Exception, e:
			print str(e), '###'
			return (filtering_urls, 0)

		if resp.status_code == 200:#or len(self.get_static_resource(self.fixed_test_url)):
			filtering_urls = pq_(resp.text)
			count =  filtering_urls('#search_header #rezsearch #search_table #result_count').text().split(' ')[0].replace(',', '')
			filtering_urls = filtering_urls('.refinement')
			if count.isdigit():
				count = int(count)
			else:
				count = 0
			resp.cookies.clear()
			return (filtering_urls, count)
		else:
			return self.get_filter_urls(init_url, counter+1)

	def get_resource_px(self, url_, counter):
		"""NEW -- PROXIED WAY OF GETTING DATA"""
		data = []
		resp = get_data(url_)
		html = pq_(resp)
		html = html('#results').children()
		for each in html:
			data.append(pq_(each).attr('id'))
		return data

	def get_resource(self, routes_container, counter):
		if counter >= self.max_recursion_depth:
			print 'max recursion depth achieved in the get_resource'
			#slp(300)
			return []

		data = []
		results = None
		try:
			unsent_request = (grequests.get(url) for url in routes_container)
			results = grequests.map(unsent_request)
		except Exception, e:
			print str(e), '@@@'
			return self.get_resource(routes_container, counter+1)
		if results:
			for resp in results:
				if resp.status_code == 200:
					html = pq_(resp.text)
					html = html('#results').children()
					data_ = []
					for each in html:
						data_.append(pq_(each).attr('id'))
					data.append(data_)
					resp.cookies.clear()
			return data
		else:
			return data


	def get_resource2(self, url_, counter):
		if counter >= self.max_recursion_depth:
			print 'max recursion depth achieved in the get_resource'
			#slp(300)
			return []
		data = []

		try:
			user_agent = self.user_agents_cycle.next()
			resp = requests.get(url_, headers = {'user_agent': user_agent})
		except Exception, e:
			print str(e), '@@@'
			return data

		if resp.status_code == 200:#or len(self.get_static_resource(self.fixed_test_url)):
			html = pq_(resp.text)
			html = html('#results').children()
			for each in html:
				data.append(pq_(each).attr('id'))
			return data
		else:
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
				resp.cookies.clear()
				return data
			else:
				return data
		except RuntimeError:
			return data

	
	def begin(self):
		job_start_time = tm()
		#sorts = ['sort=date', '']
		keywords_done_idx = self.index
		#keywords_done_idx = self.r_master.get(self.country_code) #--this over here should talk to master's redis
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
				print 'now working on..%d in begin..' % i 
				#for sort in sorts:
				self.resource_collection(i, keyword, '')
				slp(60)
				print 'sleeping after %d for a minute to relax..' % i
				#--checking the block
				if sum(map(lambda p: p[1], self.time_all[-2:])) == 0 and len(self.time_all) > 2:
					check = self.get_static_resource(self.fixed_test_url)
					if not len(check):
						print 'putting to sleep for 10 mins because last 2 keywords went nill and check indicated block..'
						print 'currently worked at .. %d' % i
						slp(400)

				#--switching to the sibling node every 20 mins
				time_right_now = tm()
				if (time_right_now - job_start_time) >= 60*20:
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
	obj = indeed_resumes(country_code, master, index)
	obj.begin()
