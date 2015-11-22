browse_url_profiles = 'http://www.indeed.com/resumes/%s?co=US&start=%d&limit=%d'
browse_url_profile_details = 'http://www.indeed.com/r/%s'

def create(n):
	if n <= 100:
		print 0, n
	else:
		u = range(n)
		for i in range(n):
			if i % 50 == 0:
				print i, i+50
