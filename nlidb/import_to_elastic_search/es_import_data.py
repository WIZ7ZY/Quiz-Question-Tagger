# one document contains all values of one column
# no of docs = number of columns in all tables combined

import json
from elasticsearch import Elasticsearch
import unicodedata
import sys
# sys.setdefaultencoding() does not exist, here!
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')
path = '..\\data\\WikiSQL\\'
f = open(path+'dev.tables.jsonl', 'r')
# f = open(path+'testfile.tables.jsonl', 'r')

tableColPair = {}
columns = []
counter = 0
for line in f.readlines():
    data = json.loads(line)
    header = data['header']
    val = data['rows']
    tableid = data['id']
    tableColPair[tableid] = header

    for ind ,column in enumerate(header):
        dataStr = ""
        for valu in val:
            dataStr += str(valu[ind]).encode('utf-8') + " "
        dicts = {'tableid' :tableid, 'column_name': column, 'column_index': header.index(column), 'column_value': dataStr}

        rows = [dicts]
        rows_json = json.dumps(rows)
        chunk = unicodedata.normalize('NFKD', unicode(rows_json, 'utf-8', 'ignore')).encode('ASCII', 'ignore').replace(
            '[', '').replace(']', '').replace('"null"', 'null').replace('999999.99', '0.0')

        es = Elasticsearch(host='localhost', port=9200)
        res = es.index(index="final_data", doc_type='final_data', body=chunk, ignore=400)
        # res = es.index(index="test_full_data", doc_type='test_full_data', body=chunk, ignore=400)
        if counter % 10 == 0:
            print "completed " + str(counter + 1) + " documents"
        counter = counter + 1