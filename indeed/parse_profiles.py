import sys, os, requests, random, string, json, locale, gc, socket, subprocess
#import grequests
from data_getter import get_data
from time import time as tm, sleep as slp
from itertools import cycle

import skiff
from pyquery import PyQuery as pq_

from cities import countries
from configs import configs
from parse_profile_details import indeed_resumes_details
from db_access import db_insert_hash, get_distincts, id_exists

from user_agents import user_agents

data_dir = '../../data/'
locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' ) 
skillset = configs['skills_file'] #--change this to change the filename

#--this was previously test token
token = 'b57edf366525324117fdcf42a1fe433327763ecae070c9ac01519ff4e5b0dab3'

def get_all_nodes():
	drops = {}
	skiffer = skiff.rig(token)
	for k in skiffer.Droplet.all():
		name = k.name
		for i in k.v4:
			ip_address = i.ip_address
		if name != 'indeed-master-01':
			drops[name] = ip_address
	return drops

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
		self.final_all = 0

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
	  	out = filter(lambda p: len(p), out)
	  	return out

	def dispatch(self, data, keyword, index):
		directory = '../../data/chunks/%s' % keyword+'-'+str(index)
		if not os.path.exists(directory):
			os.makedirs(directory)
		command = 'scp -o "StrictHostKeyChecking no" %s root@%s:data/collect/%s'
		data = self.chunk_it(data, 4)
		files = []
		#--save all the chunks
		for each in data:
			print each
			print '###'
			name = ''.join(random.choice(string.lowercase) for x in range(10))+'.txt'
			filepath = directory+'/'+name
			f = open(filepath, 'wb')
			for i in each:
				f.write(i+'\n')
			f.close()
			files.append([filepath, name])

		drops = get_all_nodes()
		for g, k in enumerate(drops):
			command_ = command % (files[g][0], drops[k], files[g][1])
			execute = call([command], shell=True)
		return
			


	def resource_collection(self, keyword_index, keyword, sort, rest_kewords=False):
		start_time = tm()
		n_profiles = {}
		keyword = '%s' % keyword.replace('/', ' ')
		keyword = keyword.strip('\n')
		init_url = self.init_url % (keyword.replace(' ', '+'), 0, 50)
		filtering_urls, result_count = self.get_filter_urls_px(init_url, 0)
		print result_count, type(result_count)
		if result_count < 500:
			return

		for route in filtering_urls:
			if len(n_profiles) >= 300:
				break
			t_res1 = tm()
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
			for i in range(counter):
				if i == 0:
					beg = i
					end = i+100
				else:
					beg = end
					end = end+100
				postfix = '&start=%d&limit=%d&radius=100&%s&co=%s' % (beg, end, sort, self.country_code)
				print url_+postfix
				data_ = self.get_resource_px(url_+postfix, 0)
				slp(3)
				for unique_id in data_:
					n_profiles[unique_id] = True

			t_res2 = tm()
			print 'data is here in %f secs' % float(t_res2 - t_res1)
			print 'till now, the set is .. %d ids --> %s (%d)' % (len(n_profiles), keyword, keyword_index)
			slp(10)
			self.final_all += len(n_profiles)
			gc.collect()
		current_time = tm()
		print 'current time passed..%d secs for one round of %s (%d)' % (int(current_time - begin_time), keyword, keyword_index)
		print 'total records collected for %s (%d) --> %d' % (keyword, keyword_index, len(n_profiles))
		print 'begin dispatching..'
		self.dispatch(n_profiles.keys(), keyword, keyword_index)
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
		keywords_done_idx = self.index
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
				print 'now working on..%d, %s in begin..' % (i, keyword) 
				self.resource_collection(i, keyword, '')
				print 'total till now for all... %d' % (self.final_all)
				print 'sleeping after %d for a minute to relax..' % i
				slp(60)
		self.keywords.close()


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
