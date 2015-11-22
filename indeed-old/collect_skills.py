import sys, os, json
from time import time as tm
from itertools import cycle

import requests
from pyquery import PyQuery as pq_

from user_agents import user_agents


class collect_onet_skills(object):
	def __init__(self):
		self.url_base = 'http://www.onetonline.org/find/quick?s=%s&a=1'
		self.max_count_per_skill = 100
		self.user_agents_cycle = cycle(user_agents)
		self.domain_keywords_raw = 'keywords/domain_keywords.txt'
		self.domains = self.parse_domains()
		self.n_concepts = 0
		self.final_data = {}

	def parse_domains(self):
		data = []
		f = open(self.domain_keywords_raw, 'rb')
		for a in f:
			data.append(a.strip('\n'))
		f.close()
		return data

	def collect_keywords(self):
		t1 = tm()
		for keyword in self.domains:
			domain_name = keyword.upper().replace(' ', '_')
			if domain_name in self.final_data:
				container = self.final_data[domain_name]
			else:
				self.final_data[domain_name] = []
				container = self.final_data[domain_name]

			url_ = self.url_base % keyword.replace(' ', '+')
			data = self.get_resource(url_)
			for each in data:
				child = pq_(each).text()
				container.append(child)	
				self.n_concepts += 1
		t2 = tm()		
		f = open('keywords/skills.json', 'wb')
		f.write(json.dumps(self.final_data))
		f.close()
		print 'total time taken: %d seconds..' % int(t2-t1)
		print '%d concepts saved in keywords/skills.json' % self.n_concepts
	

	def get_resource(self, url_):
		user_agent = self.user_agents_cycle.next()
		try:
			resp = requests.get(url_, headers = {'user_agent': user_agent})
		except:
			slp(300)
			print 'sleeping for 300 secs due to a block..'
			user_agent = self.user_agents_cycle.next()
			resp = requests.get(url_, headers = {'user_agent': user_agent})

		if resp.status_code == 200:
			data = pq_(resp.text)
			data = data('.report2ed')
			if not data:
				user_agent = self.user_agents_cycle.next()
				resp = requests.get(url_, headers = {'user_agent': user_agent})
				if resp.status_code == 200:
					data = pq_(resp.text)
					data = data('.report2ed')
					return data
				else:
					return []
			else:
				return data
		else:
			return []



if __name__ == '__main__':
	obj = collect_onet_skills()
	obj.collect_keywords()
