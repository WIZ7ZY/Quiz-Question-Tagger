# Imports table names and column names
# each document contains names of all the columns of one table
# number of docs = Number of tables

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
    tableColPair[tableid] = header
    headerStr = ""
    for column in header:
        headerStr += column + " "
    dicts = {'tableid':tableid, 'column_name': headerStr}
    rows = [dicts]
    rows_json = json.dumps(rows)
    chunk = unicodedata.normalize('NFKD', unicode(rows_json, 'utf-8', 'ignore')).encode('ASCII', 'ignore').replace(
        '[', '').replace(']', '').replace('"null"', 'null').replace('999999.99', '0.0')

    es = Elasticsearch(host='localhost', port=9200)
    res = es.index(index="final_metadata2", doc_type='final_metadata2', body=chunk, ignore=400)
    # res = es.index(index="metadata2", doc_type='metadata2', body=chunk, ignore=400)
    if counter % 10 == 0:
        print "completed " + str(counter + 1) + " documents"
    counter = counter + 1
print "completed " + str(counter) + " documents"

