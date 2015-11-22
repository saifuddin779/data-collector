import sys, os
import socket
import redis
import sqlite3 as  sql
from time import time as tm

hostname = socket.gethostname()
master_db_file = '../data/%s.db' % hostname
droplet_dbs_folder = '../data/droplets_dbs/'
r_master = redis.StrictRedis('localhost', '6379') 


def generate_table():
	"""
	GENERATES THE RESUME IDS TABLE -- ONE SHOT METHOD
	"""
	con = sql.connect(master_db_file, timeout=10)
	cur = con.cursor()
	queries = [
				"CREATE TABLE IF NOT EXISTS indeed_resumes(id INTEGER PRIMARY KEY AUTOINCREMENT, indeed_id VARCHAR, city_name VARCHAR, country_code INT NOT NULL);",
		   		"CREATE UNIQUE INDEX indeed_id_idx ON indeed_resumes(indeed_id, city_name, country_code);"
		   	]
	for query in queries:
		cur.execute(query)
	con.commit()
	con.close()
	return


def drop_table():
	"""
	TRUNCATES MASTER DB -- DELETES THE TABLE FOR RESUME IDS
	"""
	query = "drop table indeed_resumes;"
	con = sql.connect(master_db_file, timeout=10)
	cur = con.cursor()
	cur.execute(query)
	con.commit()
	con.close()
	return


def ingest_(droplet_db_file, master_cur, master_con):
	"""
	INGESTS DATA FROM THE DROPLET DB TO MASTER DB
	"""
	query = "select indeed_id, city_name, country_code from indeed_resumes;"
	con = sql.connect(droplet_dbs_folder+droplet_db_file, timeout=10)
	cur = con.cursor()
	cur.execute(query)
	for indeed_id, city_name, country_code in cur:
		master_cur.execute("INSERT OR REPLACE INTO indeed_resumes (indeed_id, city_name, country_code) VALUES (?, ?, ?);", (indeed_id, city_name, country_code))
	con.close()
	master_con.commit()
	return


def converge_droplets_dbs():
	"""
	CONVERGES ALL THE DBS TO A SINGLE MASTER DB
	"""
	t1 = tm()
	master_con = sql.connect(master_db_file, timeout=10)
	master_cur = master_con.cursor()
	for root, directories, files in os.walk(droplet_dbs_folder):
		for filename in files:
			splitted = filename.split('.')
			if len(splitted) > 1 and splitted[1] == 'db':
				if not r_master.hget('dbs_done', filename):
					r_master.hset('dbs_done', filename, True)
					print filename
					ingest_(filename, master_cur, master_con)
	master_con.close()
	t2 = tm()
	print 'total time taken for converging .. %d' % int(t2-t1)
	return

def clear_redis():
	for root, directories, files in os.walk(droplet_dbs_folder):
		for filename in files:
			splitted = filename.split('.')
			if len(splitted) > 1 and splitted[1] == 'db':
				r_master.hdel('dbs_done', filename)
	return


if __name__ == '__main__':
	command = sys.argv[1]
	if command == 'generate':
		generate_table()
	if command == 'converge':
		converge_droplets_dbs()
	if command == 'truncate':
		drop_table()
	if command == 'clear_redis':
		clear_redis()



