import sys, os, requests, json
from time import time as tm, sleep as slp
from itertools import cycle

from pyquery import PyQuery as pq_

from browse import browse_url_profiles
#from skills import skillset
from skillsets.skills import get_skills
from user_agents import user_agents

data_dir = 'data/'
skillset = get_skills('v2') #--getting the scrapped ones from onet

class indeed_resumes(object):
	def __init__(self, area, keywords):
		self.area = area
		self.keywords = keywords
		self.directory = data_dir+'%s/' % self.area
		if not os.path.exists(self.directory):
			os.makedirs(self.directory)
			print '%s created..' % self.directory
		self.user_agents_cycle = cycle(user_agents)
		self.n_distinct = {}
		self.n_done = []

	def resource_collection(self, keyword):
		t1 = tm()
		n_profiles = 0
		keyword = keyword.replace('/', ' ')
		file_ = open(os.path.join(os.path.dirname(self.directory), keyword+'.json'), 'a+')
		#--lets aim for collecting 1000+ profiles per skill/keyword
		for i in range(15):
			if i == 0:
				beg = i
				end = i+100
			else:
				beg = end
				end = end+100
			url_ = browse_url_profiles % (keyword, beg, end)
			data = self.get_resource(url_)

			for each in data:
				item = pq_(each)
				unique_id = item.attr('id')
				item_data = self.get_info(item('.sre-content'))
				item_data.append({'type': 'resource_id', 'data': unique_id})
				if unique_id not in self.n_distinct:
					self.n_distinct[unique_id] = 1
				file_.write(json.dumps(item_data)+'\n')
				n_profiles += 1
				# if n_profiles % 500 == 0:
				# 	print "%d profiles collected for %s - %s" % (n_profiles, self.area, keyword)

		file_.close()
		t2 = tm()
		print "done collecting %d records  for (%s - %s) ..in %d seconds.." % (n_profiles, self.area, keyword, int(t2-t1))
		print "TOTAL DISTINCT: %d " %  len(self.n_distinct)
		print "\n"
		self.n_done.append(self.area)
		return

	def get_resource(self, url_):
		user_agent = self.user_agents_cycle.next()

		#--add some more solid step here, as in the except when the max retries error occurs, the script breaks
		try:
			resp = requests.get(url_, headers = {'user_agent': user_agent})
		except:
			slp(300)
			print 'sleeping for 300 secs due to a block..'
			user_agent = self.user_agents_cycle.next()
			resp = requests.get(url_, headers = {'user_agent': user_agent})

		if resp.status_code == 200:
			data = pq_(resp.text)
			data = data('#results').children()
			if not data:
				user_agent = self.user_agents_cycle.next()
				resp = requests.get(url_, headers = {'user_agent': user_agent})
				if resp.status_code == 200:
					data = pq_(resp.text)
					data = data('#results').children()
					return data
				else:
					return []
			else:
				return data
		else:
			return []

	def get_info(self, item):
		item_data = []
		for i in item.children():
			entry = pq_(i)
			class_name = entry.attr('class')
			if class_name == 'app_name':
				nameloc = entry.text().split('-')
				name = nameloc[0]
				location = nameloc[-1]
				item_data.append({'type': 'name', 'data': name})
				item_data.append({'type': 'location', 'data': location})
			else:
				item_data.append({'type': class_name, 'data': entry.text()})
		return item_data

	def begin(self):
		for keyword in self.keywords:
			self.resource_collection(keyword)


if __name__ == '__main__':
	distincts = {}
	for area, keywords in skillset.iteritems():
		directory = data_dir+'%s/' % area
		if not os.path.exists(directory):
			obj = indeed_resumes(area, keywords[0:30])
			obj.begin()
			for k in obj.n_distinct:
				distincts[k] = 1
			#distincts = len(obj.n_distinct)
			print '##########DISTINCTS: %d ########' % distincts
			print '\n'


