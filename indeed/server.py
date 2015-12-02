import sys, os, ast, json, requests
from subprocess import call, Popen, PIPE, STDOUT
from flask import Flask, render_template

app = Flask(__name__)
app.debug = True

@app.route('/')
def index():
	return 'index page'

@app.route('/begin/')
def begin():
	index = int(requests.args['index'])
	country_code = requests.args['country_code'].encode('utf-8')
	command = "tmux send -t scrap_session.0 'python parse_profiles.py %s prod %d' ENTER" % (country_code, index)
	p = Popen(command, shell=True, stdout=PIPE, stderr=STDOUT, bufsize=1, close_fds=True)
	return {'status': True}

if __name__ == '__main__':
	app.run()

