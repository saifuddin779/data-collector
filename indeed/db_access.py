import sys
import redis
import sqlite3 as  sql
import socket

hostname = socket.gethostname()
db_file = '../../data/%s.db' % hostname

def generate_table():
	"""
	GENERATES THE RESUME IDS TABLE -- ONE SHOT METHOD
	"""
	con = sql.connect(db_file, timeout=10)
	cur = con.cursor()
	
	queries = ["CREATE TABLE IF NOT EXISTS indeed_resumes(id INTEGER PRIMARY KEY AUTOINCREMENT, indeed_id VARCHAR, city_name VARCHAR, country_code INT NOT NULL);",
		   "CREATE UNIQUE INDEX indeed_id_idx ON indeed_resumes(indeed_id, city_name, country_code);"]
	for query in queries:
		cur.execute(query)
	con.commit()
	con.close()


def truncate_table(table_name):
	"""
	TRUNCATES THE GIVEN TABLE NAME
	"""
	query = "delete from %s;" % table_name
	con = sql.connect(db_file, timeout=10)
	cur = con.cursor()
	cur.execute(query)
	con.commit()
	con.close() 

def drop_table(table_name):
	"""
	DROPS THE TABLE ALONG WITH DATA/INDICES
	"""
	query = "drop table %s;" % table_name
	con = sql.connect(db_file, timeout=10)
	cur = con.cursor()
	cur.execute(query)
	con.commit()
	con.close()

def db_insert_hash(hash_, country_code):
	"""
	INSERTS THE ROWS FROM WITHIN A REGULAR HASH
	"""
	country_mapping = {'US': 1, 'IN': 2, 'AU': 3, 'GB': 4, 'CA': 5}
	country_id = country_mapping[country_code]
	con = sql.connect(db_file)
	cur = con.cursor()
	for k, v in hash_.iteritems():
		cur.execute("INSERT OR REPLACE INTO indeed_resumes (indeed_id, city_name, country_code) VALUES (?, ?, ?);", (k, v, country_id))
	con.commit()
	con.close()	

def db_insert(redis_host):
	"""
	INSERTS THE ROWS PROVIDED INTO THE GIVEN REDIS HASH
	"""
	con = sql.connect(db_file)
	cur = con.cursor()
	for k, v in redis_host.hscan_iter('distincts'):
		cur.execute("INSERT OR REPLACE INTO indeed_resumes VALUES (NULL, ?);", (k,))
		#cur.execute(query)
	con.commit()
	con.close()

def clear_redis():
	"""
	FLUSHES THE REDIS DB
	"""
	r = redis.StrictRedis('localhost', '6379')
	r.flushdb()
	return

def get_distincts(country_code):
	"""
	RETURNS DISTINCT RECORDS FOR THE GIVEN COUNTRY CODE
	"""
	country_mapping = {'US': 1, 'IN': 2, 'AU': 3, 'GB': 4, 'CA': 5}
	country_id = country_mapping[country_code]
	query = "SELECT count(id) from indeed_resumes where country_code = %d;" % country_id
	con = sql.connect(db_file)
	cur = con.cursor()
	cur.execute(query)
	distinct_count = cur.fetchall()
	con.close()
	return distinct_count

def get_distincts_all():
	"""
	RETURNS COUNT FOR ALL THE RECORDS
	"""
	query = "SELECT count(*) from indeed_resumes;"
	con = sql.connect(db_file)
	cur = con.cursor()
	cur.execute(query)
	distinct_count = cur.fetchall()
	con.close()
	return distinct_count


if __name__ == '__main__':
	if len(sys.argv) > 1:
		command = sys.argv[1]
		if command == 'generate':
			generate_table()
		elif command == 'regenerate':
			drop_table('indeed_resumes')
			generate_table()
		

#clear_redis()
#drop_table('indeed_resumes')
#generate_table()
#truncate_table('indeed_resumes')
#print get_distincts_all()
