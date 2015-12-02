import sys, os, ast, json
from flask import Flask, render_template

app = Flask(__name__)
app.debug = True

@app.route('/')
def index():
	return 'index page'


if __name__ == '__main__':
	app.run()

