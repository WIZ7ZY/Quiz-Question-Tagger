# Creator: Avikalp Kumar Gupta <avikalp.gupta@freshgravity.com>

import sqlite3

class Execution(object):
	conn = ""

	def __init__(self, path_to_sqlite_db):
		self.conn = sqlite3.connect(path_to_sqlite_db) 

	def execute(self, query, exec_type=None):
		# if type:
		# 	# redirect to corresponding execution function
		# 	eval_string = exec_type + "(" + query + ")"
		# 	eval(eval_string)
		# else:
		cursor = self.conn.execute(query)
		return self.pretty_print(cursor)
			
	def pretty_print(self, cursor):
		output_string = ""
		for row in cursor.fetchall():
			for value in row:
				output_string += str(value) + "\t"
		output_string += "\n"
		return output_string

	def desc(self, tablename):
		cursor = self.conn.cursor()
		desc_query = "PRAGMA table_info(" + tablename + ");"
		cursor.execute(desc_query)
		return cursor.fetchall()
