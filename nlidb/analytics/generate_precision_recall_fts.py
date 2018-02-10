import json
from nltk.corpus import wordnet as wn
from nltk.tag.stanford import StanfordPOSTagger as PTag
import nltk
import sys
from elasticsearch import Elasticsearch
import numpy as np

from nltk.corpus import wordnet as wn
from nltk.stem.wordnet import WordNetLemmatizer
import operator

import re, string; 


reload(sys)
sys.setdefaultencoding('utf-8')

tagger = PTag("../SQLFlask/postagger/models/english-bidirectional-distsim.tagger","../SQLFlask/postagger/stanford-postagger.jar")
lmtzr = WordNetLemmatizer()

testdata_folder = '../data/WikiSQL/'

es = Elasticsearch(host='localhost', port=9200)

def posTags(s):
	return tagger.tag(s.split())

def analysis():

	f = open(testdata_folder + 'testfile.tables.jsonl', 'r')
	tables = []
	tableColPair = {}
	columns = []
	countpos=0
	countneg=0

	i=0
	#Load all the table column pairs from the database.
	for line in f.readlines():
		data = json.loads(line)
		header = data['header']
		tableid = data['id']
		for col in header:
			columns.append(col)
		tableColPair[tableid] = header
		i =i +1

	f2 = open(testdata_folder + 'testfile.jsonl', 'r')
	dataset = []
	for line in f2.readlines():
		# data = json.loads(line)
		try:
			data = json.loads(line)
			dataset.append(data)
		except:
			pass
	# processWithPosTagsIntersectionFTS(dataset, tableColPair, "metadata2", "test_full_data")
	# processWithPosTagsIntersectionFTS(dataset, tableColPair, "metadata2", "final_data")
	processWithPosTagsAndIntersection(dataset, tableColPair, "final_data")
	# processWithPosTags(dataset, tableColPair, "test_data")
	# processWithoutPosTags(dataset, tableColPair, "metadata2")

def processWithPosTagsIntersectionFTS(dataset, tableColPair, search_metadata_schema, search_schema):

	total = 0.0
	correctAtK = np.zeros(10)
	dataset_size = len(dataset)
	for data in dataset:
		print "\rProgress: ", total, "/", dataset_size,
		total = total + 1
		question = data['question']
		sel_col_index = data['sql']['sel']
		table = data['table_id']

		if (table in tableColPair):		
			#We need to do analysis on question here
			sel_col = str(tableColPair[table][sel_col_index])
			# question = question[:-1] #Assuming ?/full stop

			# Replacing all the non alpha-numeric characters from the question by space
			pattern = re.compile('[\W_]+')
			question = pattern.sub(' ', question)

			# POS Tagging each word in the sentence
			tags = posTags(question)

			# converting all words to lower case - beneficial only for English
			for i in range(len(tags)):
				tags[i] = (tags[i][0].decode('utf-8').lower(),tags[i][1])
			
			nouns = ""
			allResults = {}
			allResultsDict = {}
			# print table
			for (word, tag) in tags:
				if tag in ['NN', 'NNS', 'JJ', 'VB']:
					res = es.search(index=search_metadata_schema,
						body={"query": {"query_string": {"query": word}}})
					results = res['hits']['hits']
					# print "Average Results: " + str(len(results))
					for result in results:
						allResults[result['_source']['tableid']] = result
						if allResultsDict.has_key(result['_source']['tableid']):
							allResultsDict[result['_source']['tableid']]+=1
						else:
							allResultsDict[result['_source']['tableid']]=1
			for (word, tag) in tags:
				if tag in ['NN', 'NNS', 'JJ', 'VB']:
					res = es.search(index=search_schema,
						body={"query": {"query_string": {"query": word}}})
					results = res['hits']['hits']
					# print "Average Results FTS: " + str(len(results))
					for result in results:
						tableid = result['_source']['column_name']
						tableid = tableid[6:]
						if allResultsDict.has_key(tableid):
							allResultsDict[tableid]+=1
						else:
							allResultsDict[tableid]=1
						#Assuming for now we will have column names in result colum
			# if table in allResultsDict.keys():
			# 	print "Yes Table Found"
			sorted_results = sorted(allResultsDict.items(), key=operator.itemgetter(1), reverse=True)
			# print sorted_results
			found_flag = 0
			it = 0
			for tableid, val in sorted_results:
				if tableid not in allResults.keys():
					continue
				result = allResults[tableid]
				if result['_source']['tableid'] == table and sel_col.lower() in result['_source']['column_name'].lower():
					found_flag = 1
				if found_flag:
					correctAtK[it]+=1
				it+=1
				# Truncating considered results to just top 10
				if it < len(correctAtK):
					continue
				else:
					break
			while it < len(correctAtK) and found_flag:
				correctAtK[it]+=1
				it+=1
		else:
			continue
			# exit()
	print total
	print correctAtK
	for correct in correctAtK:
		print correct/total


def processWithPosTagsAndIntersection(dataset, tableColPair, search_schema):

	total = 0.0
	correctAtK = np.zeros(10)
	dataset_size = len(dataset)
	for data in dataset:
		print "\rProgress: ", total, "/", dataset_size,
		total = total + 1
		question = data['question']
		sel_col_index = data['sql']['sel']
		table = data['table_id']

		if (table in tableColPair):		
			#We need to do analysis on question here
			sel_col = str(tableColPair[table][sel_col_index])
			# question = question[:-1] #Assuming ?/full stop
			
			# Replacing all the non alpha-numeric characters from the question by space
			pattern = re.compile('[\W_]+')
			question = pattern.sub(' ', question)

			# POS Tagging each word in the sentence
			tags = posTags(question)

			# converting all words to lower case - beneficial only for English
			for i in range(len(tags)):
				tags[i] = (tags[i][0].decode('utf-8').lower(),tags[i][1])
			
			nouns = ""
			allResults = {}
			allResultsDict = {}
			# print table
			for (word, tag) in tags:
				if tag in ['NN', 'NNS', 'JJ', 'VB']:
					res = es.search(index=search_schema,
						body={"query": {"query_string": {"query": word}}})
					results = res['hits']['hits']
					# print "Average Results: " + str(len(results))
					for result in results:
						allResults[result['_source']['tableid']] = result
						if allResultsDict.has_key(result['_source']['tableid']):
							allResultsDict[result['_source']['tableid']]+=1
						else:
							allResultsDict[result['_source']['tableid']]=1
			# if table in allResultsDict.keys():
			# 	print "Yes Table Found"
			sorted_results = sorted(allResultsDict.items(), key=operator.itemgetter(1), reverse=True)
			# print sorted_results
			found_flag = 0
			it = 0
			for tableid, val in sorted_results:
				result = allResults[tableid]
				if result['_source']['tableid'] == table and sel_col.lower() in result['_source']['column_name'].lower():
					found_flag = 1
				if found_flag:
					correctAtK[it]+=1
				it+=1
				# Truncating considered results to just top 10
				if it < len(correctAtK):
					continue
				else:
					break
			while it < len(correctAtK) and found_flag:
				correctAtK[it]+=1
				it+=1
		else:
			continue
			# exit()
	print total
	print correctAtK
	for correct in correctAtK:
		print correct/total

def processWithPosTags(dataset, tableColPair, search_schema):

	total = 0.0
	correctAtK = np.zeros(10)
	dataset_size = len(dataset)
	for data in dataset:
		print "\rProgress: ", total, "/", dataset_size,
		total = total + 1
		question = data['question']
		sel_col_index = data['sql']['sel']
		table = data['table_id']

		if (table in tableColPair):
			#We need to do analysis on question here
			sel_col = str(tableColPair[table][sel_col_index])
			# question = question[:-1] #Assuming ?/full stop
			
			# Replacing all the non alpha-numeric characters from the question by space
			pattern = re.compile('[\W_]+')
			question = pattern.sub(' ', question)

			# POS Tagging each word in the sentence
			tags = posTags(question)

			# converting all words to lower case - beneficial only for English
			for i in range(len(tags)):
				tags[i] = (tags[i][0].decode('utf-8').lower(),tags[i][1])

			nouns = ""
			for (word, tag) in tags:
				if tag in ['NN', 'NNS', 'JJ', 'VB']:
						nouns += word + " "
			res = es.search(index=search_schema,
				body={"query": {"query_string": {"query": nouns}}})
			results = res['hits']['hits']
			# avg_results += len(results)
			# print "Average Results: " + str(len(results))
			found_flag = 0
			it = 0
			for result in results:
				if result['_source']['tableid'] == table and sel_col.lower() in result['_source']['column_name'].lower():
					found_flag = 1
					# print "Found"
				if found_flag:
					correctAtK[it]+=1
				it+=1
			while it < len(correctAtK) and found_flag:
				correctAtK[it]+=1
				it+=1
		else:
			continue
			# exit()
	print "total:", total
	print correctAtK
	for correct in correctAtK:
		print correct/total

def processWithoutPosTags(dataset, tableColPair, search_schema):

	total = 0.0
	relevant = 0.0
	correctAtK = np.zeros(10)
	dataset_size = len(dataset)
	for data in dataset:
		print "\rProgress: ", total, "/", dataset_size,
		total = total + 1
		question = data['question']
		sel_col_index = data['sql']['sel']
		table = data['table_id']

		if (table in tableColPair):
			relevant += 1
			#We need to do analysis on question here
			sel_col = str(tableColPair[table][sel_col_index])

			# Replacing all the non alpha-numeric characters from the question by space
			pattern = re.compile('[\W_]+')
			question = pattern.sub(' ', question)
			question = question[:-1] #Assuming ?/full stop
			res = es.search(index=search_schema,
				body={"query": {"query_string": {"query": question}}})
			results = res['hits']['hits']
			# avg_results += len(results)
			# print "Average Results: " + str(len(results))
			found_flag = 0
			it = 0
			for result in results:
				if result['_source']['tableid'] == table and sel_col.lower() in result['_source']['column_name'].lower():
					found_flag = 1
					# print "Found"
				if found_flag:
					correctAtK[it]+=1
				it+=1
			while it < len(correctAtK) and found_flag:
				correctAtK[it]+=1
				it+=1
		else:
			continue
			# exit()
	print total, relevant
	print correctAtK
	for correct in correctAtK:
		print correct/total
analysis()
