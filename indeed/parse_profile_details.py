import sys, os, json, ast, requests
from time import time as tm, sleep as slp
from datetime import datetime as dt
from itertools import cycle
import sqlite3 as  sql

from pyquery import PyQuery as pq_
from browse import browse_url_profile_details
from user_agents import user_agents


data_dir = 'profile_data/'

class indeed_resumes_details(object):
	def __init__(self, unique_id):
		self.user_agents_cycle = cycle(user_agents)
		self.unique_id = unique_id
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
		url_ = browse_url_profile_details % self.unique_id
		data = self.get_resource(url_)
		details = self.extract_details(data)
		return details


	def extract_details(self, data):
		t1 = tm()

		details = {}
		if not data:
			return details

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
		return details


	def get_resource(self, url_):
		user_agent = self.user_agents_cycle.next()
		try:
			resp = requests.get(url_, headers = {'user_agent': user_agent})
		except:
			slp(100)
			print 'sleeping for 100 secs due to a block..'
			user_agent = self.user_agents_cycle.next()
			resp = requests.get(url_, headers = {'user_agent': user_agent})

		if resp.status_code == 200:
			data = pq_(resp.text)
			data = data('#resume_body').children()
			if not data:
				user_agent = self.user_agents_cycle.next()
				resp = requests.get(url_, headers = {'user_agent': user_agent})
				if resp.status_code == 200:
					data = pq_(resp.text)
					data = data('#resume_body').children()
					return data
				else:
					return []
			else:
				return data
		else:
			return []



def save_profiles(db_file, index=False):
	increment = 200
	con = sql.connect(db_file)
	cur = con.cursor()

	if not index:
		begin_index = int([e[0] for e in cur.execute('select min(id) from indeed_resumes;')][0])
	else:
		begin_index = index

	print 'begin index is .. %d' % begin_index
	
	query = "select indeed_id from indeed_resumes order by id asc;"
	cur.execute(query)
	n_files == 0
	for i, row in enumerate(cur):
		if i <= begin_index:
			continue
		else:
			try:
				if n_files % increment == 0:
					begin_index = begin_index + increment

				if not os.path.exists(directory):
					os.makedirs(directory)

				data = indeed_resumes_details(row[0]).resource_collection()
				directory = "%d-%d" % (begin_index, begin_index+increment)

				filename = '../../data/resumes/%s/%s.json' % (directory, row[0])
				f = open(filename, 'wb')
				f.write(json.dumps(data))
				f.close()
			except Exception, e:
				print str(e)
				slp(300)
	con.close()











if __name__ == '__main__':
	obj = indeed_resumes_details('c3a2e69dd2e2ea83')
	data = obj.resource_collection()
	print data