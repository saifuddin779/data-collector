import sys, os, json, ast, random, string
from itertools import cycle
from time import sleep, time
import requests
from pyquery import PyQuery as pq_
from indeed.user_agents import user_agents


user_agents_cycle = cycle(user_agents)

def sorter(start, end):
	final = []

	##get the file contents
	f = open('indeed/SKILLS_ROUND_A.json', 'rb')
	y = []
	for a in f:
		y.append(a)
	f.close()

	#call and get the count
	for i, each in enumerate(y[start:end]):
		keyword = each.strip('\n')
		url_ = 'http://www.indeed.com/resumes?q=%s&co=US' % keyword.replace(' ', '+')
		resp = None
		while not resp:
			try:
				#user_agent = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(15))
				user_agent = user_agents_cycle.next()
				resp = requests.get(url_, headers = {'user_agent': user_agent})
			except:
				sleep(2)
				pass
		if resp.status_code == 200:
			html = pq_(resp.text)
			count = html('#search_header #rezsearch #search_table #result_count').text().split(' ')[0].replace(',', '')
			if count.isdigit():
				count = int(count)
			else:
				count = 0
			print (keyword, count)
			final.append((keyword, count))
			print i
		sleep(3)

	#reverse sort the big list
	final = sorted(final, key=lambda n: n[1], reverse=True)
	f = open('sorted_items_%d_%d.json' % (start, end), 'wb')
	for i in final:
		f.write(str(i[0])+'\t'+str(i[1])+'\n')
	f.close()
	print '%d, %d done...' % (start, end)

if __name__ == '__main__':
	start = int(sys.argv[1])
	end = int(sys.argv[2])
	sorter(start, end)
