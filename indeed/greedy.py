import sys, os
from time import sleep

from parse_profile_details import indeed_resumes_details

def greedy():
	directory = '../../data/collect/'
	while True:
		directory_list = os.listdir(directory)
		if directory_list:
			for i in directory_list:
				f = open(directory+'%s' % i, 'rb')
				for each in f.readlines():
					if each:
						obj = indeed_resumes_details(each.encode('utf-8'))
						data = obj.resource_collection()
						obj.save_to_disk(data, each)
						print '-->', each, len(each)
						sleep(0.7)
				f.close()
				os.remove(directory+'%s' % i)
		else:
			print 'waiting for the dispatch..'
			sleep(25)

if __name__ == '__main__':
	greedy()

