import sys, os, ast

data_dir = 'data/'

def get_profile_count():
	n_profiles = {}
	for root, directories, files in os.walk(data_dir):
		for filename in files:
			if filename.split('.')[1] == 'json':
				f_name = filename.split('.')[0]
				filepath = os.path.join(root, filename)
				try:
					f = open(filepath, 'rb')
					for record in f:
						record = filter(lambda p: p['type'] == 'resource_id', ast.literal_eval(record))
						for i in record:
							if i['data'] in n_profiles:
								n_profiles[i['data']] += 1
							else:
								n_profiles[i['data']] = 1
					f.close()
				except:
					f.close()
					continue
	total_records = sum(n_profiles.values())
	total_unique_records = len(set(n_profiles.keys()))
	print "total number of profile records: %d" % total_records
	print "total distinct number of profile records: %d" % total_unique_records
	return


def get_count_of_done_files():
	n = 0
	data_dir = '/Volumes/SKILLZEQ/'
	for root, directories, files in os.walk(data_dir):
		for filename in files:
			filepath = os.path.join(root, filename)
			print filepath
			return
			f = open(filepath, 'rb')
			for a in f:
				print a
			f.close()
			n += 1
			if n == 10:
				break
	return n

if __name__ == '__main__':
	#get_profile_count()
	n = get_count_of_done_files()
