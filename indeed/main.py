import json, socket

import redis
import requests
from flask import Flask

from db_access import get_distincts_all

#--running a very simple, light weight flask app to report all the numbers
app = Flask(__name__)
app.debug = True


@app.route('/')
def index():
	return "index not set yet.."

@app.route('/distinct_counts/')
def distinct_counts(non_json=False):
	d_c = get_distincts_all()
	if non_json:
		return d_c[0]
	else:
		return json.dumps(d_c[0])
	
@app.route('/scrap_counts/')
def scrap_counts():
	response = []
	hosts = {'indeed-01': '162.243.57.5', 'indeed-02': '162.243.58.204'}
	my_name = socket.gethostname()
	for k, v in hosts.iteritems():
		if k == my_name:
			response.append([k, distinct_counts(True)])
		else:
			url_ = 'http://%s:5000/distinct_counts/' % v	
			resp = requests.get(url_)
			if resp.status_code == 200:
				response.append([k, resp.json()])
	return json.dumps(response)


if __name__ == '__main__':
	app.run(host='0.0.0.0')
