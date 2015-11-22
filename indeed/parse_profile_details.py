import sys, os, json, ast, requests
from time import time as tm, sleep as slp
from datetime import datetime as dt
from itertools import cycle

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

		details['name'] = data('#basic_info_row #basic_info_cell #resume-contact').text()
		details['title'] = data('#basic_info_row #basic_info_cell #headline').text()
		details['address'] = data('#basic_info_row #basic_info_cell #contact_info_container .adr #headline_location').text()
		details['skills'] = data('.skills-content #skills-items .data_display .skill-container').text().split(',')
		details['additional_info'] = data('.additionalInfo-content #additionalinfo-items .data_display').text().encode('ascii','ignore')

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
							item_construct = children(each).text().split('-')
							for sub, index in splits.iteritems():
								data_[sub] = item_construct[index]
						else:
							data_[each] = children(each).text().encode('ascii','ignore')

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



def save_distincts():
	"""
	This method parses the unique ids from the given 
	data directory of ids scrapped from indeed
	"""
	t1 = tm()
	object_ = {}
	data_dir = 'data/'
	#export_folder = '/Volume/SKILLZEQ/resumes_v1/%s/%s/'
	export_folder = '/Volume/SKILLZEQ/resumes_v1/%s/%s/'
	target = 'profile_data/distincts_v2.json'
	target_file = open(target, 'wb')
	for root, directories, files in os.walk(data_dir):
		for filename in files:
			file_ = filename.split('.') #--complete filename
			file_format = file_[1] #--.json
			keyword = file_[0] #--file name
			domain = root.split('/')[1] #--parent folder
			if file_format == 'json':
				filepath = os.path.join(root, filename)
				f = open(filepath, 'rb')
				for record in f:
					try:
						record = filter(lambda p: p['type'] == 'resource_id', ast.literal_eval(record))
						for i in record:
							unique_id = i['data']
							if unique_id in object_:
								object_[unique_id].append(keyword)
							else:
								object_[unique_id] = [keyword]
							#object_[unique_id] = 1
					except:
						print filepath 
						continue
				f.close()
	target_file.write(json.dumps(object_))
	target_file.close()
	t2 = tm()
	print '%d seconds taken..' % int(t2-t1)
	return

def get_distincts():
	"""
	This method returns the parsed dict of the unique file generated from save_distincts
	"""
	target = 'profile_data/distincts_v2.json'
	f = open(target, 'rb')
	for a in f:
		data = json.loads(a)
	f.close()
	print 'data fetched for resume links..'
	return data

def scrap_profiles(load_done=False):
	done_ = {}
	done_target = 'profile_data/done_v1.json'
	t1 = tm()
	data = get_distincts()
	#folder = '/Volumes/SKILLZEQ/%s.json'
	folder = '/Users/saif/skillz_eq_samples/%s.json'
	for i, key in enumerate(data):
		if key not in done_:
			try:
				obj = indeed_resumes_details(key)
				profile = obj.resource_collection()
				profile['semantics'] = data[key]
			except:
				print 'put to sleep for 300 secs due to break..'
				slp(300)
				try:
					obj = indeed_resumes_details(key)
					profile = obj.resource_collection()
					profile['semantics'] = data[key]
				except:
					for k_ in data:
						if k_ not in done_:
							done_[k_] = 0
					df = open(done_target, 'wb')
					df.write(json.dumps(done_))
					df.close()
					print 'script terminated at %d records...data for dones in %s' % (i, done_target)

			f = open(folder % key, 'wb')
			f.write(json.dumps(profile))
			f.close()
			done_[key] = 1

			if i % 1000 == 0:
				t2 = tm()
				print '%d records saved in %d seconds..' % (i, int(t2-t1))
				
				if i == 2000:
					break
	t2 = tm()
	print 'success... %d records scrapped.. in %d mins..' % (i, int(float(t2-t1)/60))
	return






if __name__ == '__main__':
	scrap_profiles()
	# get_distincts()
	# save_distincts()
	# get_ids()