import os, json, ast


def get_skills(version):
	dir = os.path.dirname(__file__)
	file_name = os.path.join(dir, 'skills_%s.json' % version)	
	f = open(file_name, 'rb')
	data = [a for a in f]
	f.close()
	data = ast.literal_eval(a)
	return data
