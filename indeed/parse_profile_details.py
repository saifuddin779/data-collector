import sys, os, json, ast, requests, grequests
from time import time as tm, sleep as slp
from datetime import datetime as dt
from compiler.ast import flatten
from itertools import cycle
import sqlite3 as  sql

from pyquery import PyQuery as pq_
from browse import browse_url_profile_details
from user_agents import user_agents


data_dir = 'profile_data/'

class indeed_resumes_details(object):
	def __init__(self, unique_ids):
		self.user_agents_cycle = cycle(user_agents)
		self.unique_ids = unique_ids
		self.max_recursion_depth = 50
		self.profile_identities = {
			'workExperience': {'list_key': 'work_experiences',  
								'content': '.workExperience-content .items-container', 
								'item_w_id': '.workExperience-content .items-container #%s',
								'items': [('.work_title', None), ('.work_dates', None), ('.work_description', None), ('.work_company', {'.work_company': 0, '.work_location': -1})]
							}, 
			'education':  {'list_key': 'education_bgs',
							'content': '.education-content .items-container',
							'item_w_id': '.education-content .items-container #%s',
							'items': [('.edu_title', None), ('.edu_school', None), ('.edu_dates', None)]
							},

		}


	def resource_collection(self):
		#url_ = browse_url_profile_details % self.unique_id
		profiles_parsed_final = []
		urls_ = map(lambda k: browse_url_profile_details % k, self.unique_ids)
		urls_ = self.chunk_it(urls_, 10)
		urls_ = filter(lambda p: len(p), urls_)
		print urls_
		for urls_chunks in urls_:
			profiles_html = self.get_resource(urls_chunks, 0)
			profiles_parsed = self.extract_details(profiles_html)
			for each_profile in profiles_parsed:
				profiles_parsed_final.append(profiles_parsed)
		return flatten(profiles_parsed_final)


	def extract_details(self, profiles_html):
		t1 = tm()
		profiles_parsed = []
		if not profiles_html:
			return profiles_parsed

		for i, data in enumerate(profiles_html):
			details = {}
			details['unique_id'] = self.unique_ids[i]
			details['name'] = data('#basic_info_row #basic_info_cell #resume-contact').text().strip('\n')
			details['title'] = data('#basic_info_row #basic_info_cell #headline').text().strip('\n')
			details['address'] = data('#basic_info_row #basic_info_cell #contact_info_container .adr #headline_location').text().strip('\n')
			details['skills'] = data('.skills-content #skills-items .data_display .skill-container').text().strip('\n').split(',')
			details['additional_info'] = data('.additionalInfo-content #additionalinfo-items .data_display').text().strip('\n').encode('ascii','ignore')

			identities = {}
			for k, v in self.profile_identities.iteritems():
				identities[k] = {'data': []}
				for item in data(v['content']).children():
					data_= {}
					it = pq_(item)
					if it.attr('id').startswith(k):
						it_id = it.attr('id')
						item = data(v['item_w_id'] % it_id)
						children = pq_(item.children())
						for each, splits in v['items']:
							if splits:
								item_construct = children(each).text().strip('\n').split('-')
								for sub, index in splits.iteritems():
									data_[sub] = item_construct[index].strip('\n')
							else:
								data_[each] = children(each).text().encode('ascii','ignore').strip('\n')

					identities[k]['data'].append(data_)
				details[k] = identities[k]
			t2 = tm()
			details['time_taken'] = t2-t1
			details['timestamp'] = tm()
			profiles_parsed.append(details)
		return profiles_parsed

	def chunk_it(self, seq, num):
		avg = len(seq) / float(num)
	  	out = []
	  	last = 0.0

	  	while last < len(seq):
	  		out.append(seq[int(last):int(last + avg)])
	  		last += avg
	  	return out

	def get_resource(self, urls_, counter):
		if counter >= self.max_recursion_depth:
			print 'max recursion depth achieved in the profile get_resource'
			return []
		data = []
		results = None
		try:
			unsent_request = (grequests.get(url) for url in urls_)
			results = grequests.map(unsent_request)
		except Exception, e:
			print str(e), '@@@'
			return self.get_resource(urls_, counter+1)

		if results:
			for resp in results:
				if resp:
					if resp.status_code == 200:
						data_ = pq_(resp.text)
						data_ = data_('#resume_body').children()
						data.append(data_)
			return data
		else:
			return data



	def get_resource2(self, url_):
		data = []
		try:
			user_agent = self.user_agents_cycle.next()
			resp = requests.get(url_, headers = {'user_agent': user_agent}, timeout=5)
		except Exception, e:
			print str(e), '~~~'
			return data

		if resp.status_code == 200:
			data = pq_(resp.text)
			data = data('#resume_body').children()
			if not data:
				user_agent = self.user_agents_cycle.next()
				resp = requests.get(url_, headers = {'user_agent': user_agent}, timeout=5)
				if resp.status_code == 200:
					data = pq_(resp.text)
					data = data('#resume_body').children()
					return data
				else:
					return data
			else:
				return data
		else:
			return data


def save_profiles(db_file, index=False):
	increment = 200
	root = '../../data/resumes/'
	con = sql.connect(db_file)
	cur = con.cursor()

	if not index:
		begin_index = int([e[0] for e in cur.execute('select min(id) from indeed_resumes;')][0])
	else:
		begin_index = index

	print 'begin index is .. %d' % begin_index
	
	query = "select id, indeed_id from indeed_resumes order by id asc;"
	cur.execute(query)
	n_files = 0
	for id_, indeed_id in cur:
		print id_, indeed_id, n_files
		if id_ <= begin_index:
			continue
		else:
			try:
				if n_files % increment == 0:
					begin_index = begin_index + increment

				directory = root+"%d-%d" % (begin_index, begin_index+increment)
				if not os.path.exists(directory):
					os.makedirs(directory)
				data = indeed_resumes_details(indeed_id).resource_collection()
				filename = '%s/%s.json' % (directory, indeed_id)
				f = open(filename, 'wb')
				f.write(json.dumps(data))
				f.close()
				n_files += 1
			except Exception, e:
				print str(e)
				slp(300)
	con.close()
	return



if __name__ == '__main__':
	#save_profiles('../../backup/indeed-master-01.db')
	obj = indeed_resumes_details(['c3a2e69dd2e2ea83', 'c90082b543072ed3'])
	data = obj.resource_collection()
	for i in data:
		print i
		print '--------'
