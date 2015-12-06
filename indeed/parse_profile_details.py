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

	def save_to_disk(self, data, unique_id):
		if data:
			try:
				directory = '../../data/resumes/%s' % unique_id[0:2]
			except:
				return
			if not os.path.exists(directory):
				os.makedirs(directory)
			filename = '%s/%s.json' % (directory, unique_id)
			f = open(filename, 'wb')
			f.write(json.dumps(data))
			f.close()
		return

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
		data = []
		resp = None
		try:
			user_agent = self.user_agents_cycle.next()
			resp = requests.get(url_, headers = {'user_agent': user_agent})
		except Exception, e:
			print '%s not exists.. or blocked' % url_
			slp(500)
			return data

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

# if __name__ == '__main__':
# 	#save_profiles('../../backup/indeed-master-01.db')
# 	t1 = tm()
# 	for i in range(1000):
# 		obj = indeed_resumes_details('c3a2e69dd2e2ea83')
# 		data = obj.resource_collection()
# 		obj.save_to_disk(data, 'c3a2e69dd2e2ea83')
# 		slp(.7)
# 		break
# 	t2 = tm()
# 	print t2-t1