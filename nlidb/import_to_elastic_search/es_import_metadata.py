# Imports table names and column names
# each document is a (tableid, columnname) pair
# number of docs = Number of columns in all tables combined

import json
from elasticsearch import Elasticsearch
import unicodedata
path = '..\\data\\WikiSQL\\'
f = open(path+'dev.tables.jsonl', 'r')
# f = open(path+'testfile.tables.jsonl', 'r')

tableColPair = {}
columns = []
counter =0
for line in f.readlines():
    data = json.loads(line)
    header = data['header']
    tableid = data['id']
    data_rows = data['rows']
    # print len(data_rows), len(header)
    tableColPair[tableid] = header
    for index,column in enumerate(header):
        dicts = {'tableid':tableid, 'column_name': column, 'column_index': header.index(column)}
        # print "Header column index:", header.index(column)
        rows = [dicts]
        rows_json = json.dumps(rows)
        chunk = unicodedata.normalize('NFKD', unicode(rows_json, 'utf-8', 'ignore')).encode('ASCII', 'ignore').replace(
            '[', '').replace(']', '').replace('"null"', 'null').replace('999999.99', '0.0')

        es = Elasticsearch(host='localhost', port=9200)
        res = es.index(index="final_metadata", doc_type='final_metadata', body=chunk, ignore=400)
        # res = es.index(index="test_data", doc_type='test_data', body=chunk, ignore=400)
        if counter % 10 == 0:
            print "completed " + str(counter + 1) + " documents"
        counter = counter + 1
print "completed " + str(counter) + " documents"

