from compiler.ast import flatten
import ast

def get_skills_v1():
	"""
	READING THE FIRST SKILLSET
	"""
	f = open('skills_v1.json', 'rb')
	for a in f:
		skills_v1 = ast.literal_eval(a)
	f.close()
	return skills_v1

def get_skills_v2():
	"""
	READING THE SECOND SKILLSET
	"""
	f = open('skills_v2.json', 'rb')
	for a in f:
		skills_v2 = ast.literal_eval(a)
	f.close()
	skills_v2 = dict((k, map(lambda n: n.split(','), v)) for k, v in skills_v2.iteritems())
	return skills_v2

def get_skills_v3():
	"""
	READING THE THIRD SKILLSET
	"""
	skills_v3 = []
	f = open('skills_v3.json', 'rb')
	for a in f:
		skills_v3.append(a)
	f.close()
	return skills_v3

def dump_skills():
	"""
	DUMPING THE ENTIRE SKILLS FOR FIRST ROUND - ONE SHOT METHOD
	"""
	skills_v1 = get_skills_v1()
	skills_v2 = get_skills_v2()
	skills_v3 = get_skills_v3()
	
	skills_v1 = flatten(skills_v1.values())
	skills_v2 = flatten(skills_v2.values())
	skills_v_final = flatten([skills_v1, skills_v2, skills_v3])
	skills_v_final = list(set(skills_v_final))
	
	f = open('SKILLS_ROUND_A.json', 'wb')
	for a in skills_v_final:
		f.write(a)
	f.close()
