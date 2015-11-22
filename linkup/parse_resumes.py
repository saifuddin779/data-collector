import sys, os
from time import time as tm
from itertools import cycle

import requests
from pyquery import PyQuery as pq_

from browse import browse_url_jobs
from skills import skillset
from user_agents import user_agents

data_dir = 'data/'

class linkup_resumes(object):
	def __init__(self, area, keywords):
		self.directory = data_dir+'%s/' % self.area
		if not os.path.exists(self.directory):
			os.makedirs(self.directory)
			print '%s created..' % self.directory
		self.user_agents_cycle = cycle(user_agents)
		self.url = browse_url_jobs

	



