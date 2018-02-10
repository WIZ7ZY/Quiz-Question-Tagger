# -*- coding: utf-8 -*

import re
import sys
import unicodedata
from threading import Thread
from ParsingException import ParsingException
from Query import *
from elasticsearch import Elasticsearch
from nltk.tag.stanford import StanfordPOSTagger as PTag
from nltk.stem import WordNetLemmatizer

import Execution

reload(sys)
sys.setdefaultencoding("utf-8")

es = Elasticsearch(host='localhost', port=9200)
lmtzr = WordNetLemmatizer()
search_schema = "final_data"
path_to_sqlite_db = "C:\\New Software\\nlidb\\data\\WikiSQL\\dev.db"

class WikiSQL():
    """docstring for WikiSQL
    This is class for the WikiSQL database
    So that executable queries can be generated
    """
    # def __init__(self, columns_of_select, all_tables_from, select_phrase, columns_of_where, new_where_phrase, group_by_phrase, order_by_phrase):
    #     self.es_func = ES_Functions()
    #     self.columns_of_select = columns_of_select
    #     self.all_tables_from = all_tables_from
    #     self.select_phrase = select_phrase
    #     self.columns_of_where = columns_of_where
    #     self.new_where_phrase = new_where_phrase
    #     self.group_by_phrase = group_by_phrase
    #     self.order_by_phrase = order_by_phrase

    def __init__(self):
        self.es_func = ES_Functions()

    # def check(self):
    #     print self.columns_of_select, self.all_tables_from
    #     print self.columns_of_where
    #     print "Select :", self.select_phrase
    #     print "Where  :", self.new_where_phrase
    #     print "GroupBy:", self.group_by_phrase
    #     print "OrderBy:", self.order_by_phrase

    def get_real_table_name(self, tableid):
        return "table_"+tableid.replace("-", "_")

    def get_real_column_names(self, tableid, columns):
        exe = Execution.Execution(path_to_sqlite_db)

        tablename = self.get_real_table_name(tableid)
        desc = exe.desc(tablename)
        column_names = []
        for column_name in columns:
            #print "Success:", column_name
            column_index = self.es_func.get_column_index_from_table(tableid, column_name)
            if column_index:
                real_column_name = desc[column_index]
                # print "Comparing column_index:", column_index, real_column_name[0]
                column_names.append(real_column_name[1])
        return column_names

    # def get_real_query_params(self):
    #     all_real_tables_from = []
    #     for tables_of_from in self.all_tables_from:
    #         tablename = self.get_real_table_name(tables_of_from)
    #         all_real_tables_from.append(tablename)
    #         # print "tableid:", tables_of_from, "\t tablename:", tablename
    #         select_column_names = self.get_real_column_names(tables_of_from, self.columns_of_select)
    #         # print "columns_of_select:", self.columns_of_select, "\t column_name:", select_column_names

    #     return select_column_names, all_real_tables_from, self.select_phrase, self.columns_of_where, self.new_where_phrase, self.group_by_phrase, self.order_by_phrase
        
class ES_Functions():
    def __init__(self):
        self.seperate_columns = {
            "test_full_data",
            "final_data",
            "test_data"
        }
        self.separate_tables = {
            "metadata2"
        }

    def get_column_index_from_table(self, table, column):
        table = lmtzr.lemmatize(table.decode('utf-8').lower())
        column = lmtzr.lemmatize(column.decode('utf-8').lower())
        res = es.search(index=search_schema, body= {
            "query": {
                "bool": {
                    "must": [
                        { "match": { "tableid":  table }},
                        { "match": { "column_name": column }}
                    ]
                }
            }
        })
        # print "Total number of results returned: ", res['_shards']['total']
        results = res['hits']['hits']
        if search_schema in self.seperate_columns:
            for result in results:
                # print "result table:", lmtzr.lemmatize(result['_source']['tableid'].lower()), "\t our table:", table
                # print "result column:", lmtzr.lemmatize(result['_source']['column_name'].lower()), "\t our column:", column
                if lmtzr.lemmatize(result['_source']['tableid'].lower()) == table and lmtzr.lemmatize(result['_source']['column_name'].lower()) == column:
                    return result['_source']['column_index']
        elif search_schema in self.separate_tables:
            for result in results:
                if lmtzr.lemmatize(result['_source']['tableid'].lower()) == table:
                    # TODO: Improve this code
                    columns = lmtzr.lemmatize(result['_source']['column_name'].lower())
                    columns = columns.split(" ")
                    for i in range(0, len(columns)):
                        if columns[i] == column:
                            return i
        return None

    def es_search_table(self, word):
        word = word.decode('utf-8').lower()
        wrd = lmtzr.lemmatize(word)
        res = es.search(index=search_schema, body={"query": {"query_string": {"query": wrd}}})
        results = res['hits']['hits']
        for result in results:
            if lmtzr.lemmatize(result['_source']['tableid'].lower()) == wrd:
                return result['_source']['tableid']
        return None

    def es_search_column(self, word):
        word = word.decode('utf-8').lower()
        wrd = lmtzr.lemmatize(word)
        res = es.search(index=search_schema, body={"query": {"match": {"column_name": wrd}}})
        results = res['hits']['hits']
        for result in results:
            return result['_source']['column_name']
        return None

    def es_search_where_column(self, column, table):
        word = column.decode('utf-8').lower()
        wrd = lmtzr.lemmatize(word)
        res = es.search(index=search_schema, body={"query": {"bool":{"should":[{"match": {"column_name": wrd}},{"match":{"tableid":table}}]}}})
        results = res['hits']['hits']
        for result in results:
            return result['_source']['column_name']
        return None

    def es_get_table_from_column(self, word):
        word = word.decode('utf-8').lower()
        wrd = lmtzr.lemmatize(word)
        res = es.search(index=search_schema, body={"query": {"match": {"column_name": wrd}}})
        results = res['hits']['hits']
        for result in results:
            return result['_source']['tableid']
        return None

    def es_get_all_tables(self, word):
        list_of_tab = []
        word = word.decode('utf-8').lower()
        wrd = lmtzr.lemmatize(word)
        res = es.search(index=search_schema, body={"query": {"match": {"column_name": wrd}}})
        results = res['hits']['hits']
        for result in results:
            list_of_tab.append(result['_source']['tableid'])
        return list_of_tab

    def get_tables_of_column(self, column, real=False):
        tmp_table = []
        column = column.decode('utf-8').lower()
        wrd = lmtzr.lemmatize(column)
        res = es.search(index=search_schema, body={"query": {"match": {"column_name": column}}})
        results = res['hits']['hits']
        for result in results:
            tmp_table.append(result['_source']['tableid'])
        return tmp_table

    def es_get_columns_from_table(self, word):
        all_columns = []
        word = word.decode('utf-8').lower()
        wrd = lmtzr.lemmatize(word)
        res = es.search(index=search_schema, body={"query": {"query_string": {"query": wrd}}})
        results = res['hits']['hits']
        for result in results:
            if lmtzr.lemmatize(result['_source']['tableid'].lower()) == wrd:
                all_columns += result['_source']['column_name']
        if len(all_columns) > 0:
            return all_columns
        else:
            return None

    def get_column_name_with_alias_table(self, column, table_of_from):
        #return str(table_of_from) + '.' + str(column)

        one_table_of_column = self.get_tables_of_column(column)[0]
        tables_of_column = self.get_tables_of_column(column)

        print table_of_from
        print tables_of_column
        print one_table_of_column
        if table_of_from in tables_of_column:
            return str(table_of_from) + '.' + str(column)
        else:
            return str(one_table_of_column) + '.' + str(column)

class SelectParser(Thread):
    def __init__(self, columns_of_select, tables_of_from, phrase, count_keywords, sum_keywords, average_keywords, max_keywords, min_keywords):
        Thread.__init__(self)
        self.select_objects = []
        self.columns_of_select = columns_of_select
        self.tables_of_from = tables_of_from
        self.phrase = phrase
        self.count_keywords = count_keywords
        self.sum_keywords = sum_keywords
        self.average_keywords = average_keywords
        self.max_keywords = max_keywords
        self.min_keywords = min_keywords

        self.es_func = ES_Functions()

    def run(self):
        for table_of_from in self.tables_of_from:
            self.select_object = Select()
            is_count = False
            number_of_select_column = len(self.columns_of_select)

            if number_of_select_column == 0:
                for count_keyword in self.count_keywords:
                    if count_keyword in self.phrase:
                        is_count = True

                if is_count:
                    self.select_object.add_column(None, 'COUNT')
                else:
                    self.select_object.add_column(None, None)
            else:
                select_phrases = []
                previous_index = 0
                for i in range(0,len(self.phrase)):
                    if self.phrase[i] in self.columns_of_select:
                        select_phrases.append(self.phrase[previous_index:i+1])
                        previous_index = i+1

                select_phrases.append(self.phrase[previous_index:])

                for i in range(0, len(select_phrases)):
                    select_type = None
                    phrase = ' '.join(select_phrases[i])

                    for keyword in self.average_keywords:
                        if keyword in phrase:
                            select_type = 'AVG'
                    for keyword in self.count_keywords:
                        if keyword in phrase:
                            select_type = 'COUNT'
                    for keyword in self.max_keywords:
                        if keyword in phrase:
                            select_type = 'MAX'
                    for keyword in self.min_keywords:
                        if keyword in phrase:
                            select_type = 'MIN'
                    for keyword in self.sum_keywords:
                        if keyword in phrase:
                            select_type = 'SUM'

                    '''if (i != len(select_phrases)-1) or (select_type is not None):
                        if i >= len(self.columns_of_select):
                            column = None
                        else:'''
                    column = self.es_func.get_column_name_with_alias_table(self.columns_of_select[i], table_of_from)
                    self.select_object.add_column(column, select_type)

            self.select_objects.append(self.select_object)

    def join(self):
        Thread.join(self)
        return self.select_objects

class FromParser(Thread):
    def __init__(self, tables_of_from, columns_of_select, columns_of_where, database_object):
        Thread.__init__(self)
        self.queries = []
        self.tables_of_from = tables_of_from
        self.columns_of_select = columns_of_select
        self.columns_of_where = columns_of_where
        self.database_object = database_object

        self.es_func = ES_Functions()

    def intersect(self, a, b):
        return list(set(a) & set(b))

    def difference(self, a, b):
        differences = []
        for _list in a:
            if _list not in b:
               differences.append(_list)
        return differences

    def is_direct_join_is_possible(self, table_src, table_trg):
        join = []
        pk_table_src = self.database_object.get_primary_keys_of_table(table_src)
        pk_table_trg = self.database_object.get_primary_keys_of_table(table_trg)
        match_pk_table_src_with_table_trg = self.intersect(pk_table_src, self.es_func.es_get_columns_from_table(table_trg))
        match_pk_table_trg_with_table_src = self.intersect(pk_table_trg, self.es_func.es_get_columns_from_table(table_src))

        if len(match_pk_table_src_with_table_trg) >=1:
            return [table_src, match_pk_table_src_with_table_trg[0], table_trg]
        elif len(match_pk_table_trg_with_table_src) >= 1:
            return [table_src, match_pk_table_trg_with_table_src[0], table_trg]

    '''def get_all_direct_linked_tables_of_a_table(self, table_src):
        links = []
        #for table_trg in self.database_dico:
        #if table_trg != table_src:
        if self.es_func.es_search_table(table_src):
            link = self.is_direct_join_is_possible(table_src, table_trg)
            if link is not None:
                links.append(link)
        return links'''

    def is_join(self, historic, table_src, table_trg):
        historic = historic
        links = self.get_all_direct_linked_tables_of_a_table(table_src)

        differences = []
        for join in links:
            if join[2] not in historic:
               differences.append(join)
        links = differences

        for join in links:
            if join[2] == table_trg:
                return [0, join]

        path = []
        historic.append(table_src)

        for join in links:
            result = [1, self.is_join(historic, join[2], table_trg)]
            if result[1] != []:
                if result[0] == 0:
                    path.append(result[1])
                    path.append(join)
                else:
                    path = result[1]
                    path.append(join)
        return path

    def get_link(self, table_src, table_trg):
        path = self.is_join([], table_src, table_trg)
        if len(path) > 0:
            path.pop(0)
            path.reverse()
        return path

    def unique(self, _list):
        return [list(x) for x in set(tuple(x) for x in _list)]

    def unique_ordered(self, _list):
        frequency = []
        for element in _list:
            if element not in frequency:
                frequency.append(element)
        return frequency


    def run(self):
        self.queries = []

        for table_of_from in self.tables_of_from:
            links = []
            query = Query()
            query.set_from(From(table_of_from))
            join_object = Join()
            for column in self.columns_of_select:
                if not self.es_func.es_search_column(column):
                    foreign_table = self.es_func.get_tables_of_column(column)[0]
                    join_object.add_table(foreign_table)
                    link = self.get_link(table_of_from, foreign_table)
                    links.extend(link)
            for column in self.columns_of_where:
                if not self.es_func.es_search_column(column):
                    foreign_table = self.es_func.get_tables_of_column(column)[0]
                    join_object.add_table(foreign_table)
                    link = self.get_link(table_of_from, foreign_table)
                    links.extend(link)
            join_object.set_links(self.unique_ordered(links))
            query.set_join(join_object)
            self.queries.append(query)
            if len(join_object.get_tables()) > len(join_object.get_links()):
                self.queries = None

    def join(self):
        Thread.join(self)
        return self.queries

class WhereParser(Thread):
    def __init__(self, phrases, tables_of_from, count_keywords, sum_keywords, average_keywords, max_keywords, min_keywords, greater_keywords, less_keywords, between_keywords, negation_keywords, junction_keywords, disjunction_keywords):
        Thread.__init__(self)
        self.where_objects = []
        self.phrases = phrases
        self.tables_of_from = tables_of_from
        self.count_keywords = count_keywords
        self.sum_keywords = sum_keywords
        self.average_keywords = average_keywords
        self.max_keywords = max_keywords
        self.min_keywords = min_keywords
        self.greater_keywords = greater_keywords
        self.less_keywords = less_keywords
        self.between_keywords = between_keywords
        self.negation_keywords = negation_keywords
        self.junction_keywords = junction_keywords
        self.disjunction_keywords = disjunction_keywords

        self.es_func = ES_Functions()

    def intersect(self, a, b):
        return list(set(a) & set(b))

    def predict_operation_type(self, previous_column_offset, current_column_offset):
        interval_offset = range(previous_column_offset, current_column_offset)
        if(len(self.intersect(interval_offset, self.count_keyword_offset)) >= 1):
            return 'COUNT'
        elif(len(self.intersect(interval_offset, self.sum_keyword_offset)) >= 1):
            return 'SUM'
        elif(len(self.intersect(interval_offset, self.average_keyword_offset)) >= 1):
            return 'AVG'
        elif(len(self.intersect(interval_offset, self.max_keyword_offset)) >= 1):
            return 'MAX'
        elif(len(self.intersect(interval_offset, self.min_keyword_offset)) >= 1):
            return 'MIN'
        else:
            return None

    def predict_operator(self, current_column_offset, next_column_offset):
        interval_offset = range(current_column_offset, next_column_offset)
        if(len(self.intersect(interval_offset, self.negation_keyword_offset)) >= 1) and (len(self.intersect(interval_offset, self.greater_keyword_offset)) >= 1):
            return '<'
        elif(len(self.intersect(interval_offset, self.negation_keyword_offset)) >= 1) and (len(self.intersect(interval_offset, self.less_keyword_offset)) >= 1):
            return '>'
        if(len(self.intersect(interval_offset, self.less_keyword_offset)) >= 1):
            return '<'
        elif(len(self.intersect(interval_offset, self.greater_keyword_offset)) >= 1):
            return '>'
        elif(len(self.intersect(interval_offset, self.between_keyword_offset)) >= 1):
            return 'BETWEEN'
        elif(len(self.intersect(interval_offset, self.negation_keyword_offset)) >= 1):
            return '!='
        else:
            return '='

    def predict_junction(self, previous_column_offset, current_column_offset):
        interval_offset = range(previous_column_offset, current_column_offset)
        junction = 'AND'
        if(len(self.intersect(interval_offset, self.disjunction_keyword_offset)) >= 1):
            return 'OR'
        elif(len(self.intersect(interval_offset, self.junction_keyword_offset)) >= 1):
            return 'AND'

        first_encountered_junction_offset = -1
        first_encountered_disjunction_offset = -1

        for offset in self.junction_keyword_offset:
            if offset >= current_column_offset:
                first_encountered_junction_offset = offset
                break

        for offset in self.disjunction_keyword_offset:
            if offset >= current_column_offset:
                first_encountered_disjunction_offset = offset
                break

        if first_encountered_junction_offset >= first_encountered_disjunction_offset:
            return 'AND'
        else:
            return 'OR'

    def run(self):
        number_of_where_columns = 0
        columns_of_where = []
        offset_of = {}
        column_offset = []
        self.count_keyword_offset = []
        self.sum_keyword_offset = []
        self.average_keyword_offset = []
        self.max_keyword_offset = []
        self.min_keyword_offset = []
        self.greater_keyword_offset = []
        self.less_keyword_offset = []
        self.between_keyword_offset = []
        self.junction_keyword_offset = []
        self.disjunction_keyword_offset = []
        self.negation_keyword_offset = []

        for phrase in self.phrases:
            for i in range(0, len(phrase)):
                temp = self.es_func.es_search_where_column(phrase[i],self.tables_of_from)
                if temp != None:
                        number_of_where_columns += 1
                        columns_of_where.append(temp)
                        offset_of[temp] = i
                        column_offset.append(i)
                        break
                if phrase[i] in self.count_keywords: # before the column
                    self.count_keyword_offset.append(i)
                if phrase[i] in self.sum_keywords: # before the column
                    self.sum_keyword_offset.append(i)
                if phrase[i] in self.average_keywords: # before the column
                    self.average_keyword_offset.append(i)
                if phrase[i] in self.max_keywords: # before the column
                    self.max_keyword_offset.append(i)
                if phrase[i] in self.min_keywords: # before the column
                    self.min_keyword_offset.append(i)
                if phrase[i] in self.greater_keywords: # after the column
                    self.greater_keyword_offset.append(i)
                if phrase[i] in self.less_keywords: # after the column
                    self.less_keyword_offset.append(i)
                if phrase[i] in self.between_keywords: # after the column
                    self.between_keyword_offset.append(i)
                if phrase[i] in self.junction_keywords: # after the column
                    self.junction_keyword_offset.append(i)
                if phrase[i] in self.disjunction_keywords: # after the column
                    self.disjunction_keyword_offset.append(i)
                if phrase[i] in self.negation_keywords: # between the column and the equal, greater or less keyword
                    self.negation_keyword_offset.append(i)

        for table_of_from in self.tables_of_from:
            where_object = Where()
            for i in range(0, len(column_offset)):
            	current = column_offset[i]

                if i == 0:
                    previous = 0
                else:
                    previous = column_offset[i-1]

                if i == (len(column_offset) - 1):
                    _next = 100 # put max integer in python here ?
                else:
                    _next = column_offset[i+1]

                junction = self.predict_junction(previous, current)
                column = self.es_func.get_column_name_with_alias_table(columns_of_where[i], table_of_from)
                operation_type = self.predict_operation_type(previous, current)
                value = 'OOV' # Out Of Vocabulary: feature not implemented yet
                operator = self.predict_operator(current, _next)
                where_object.add_condition(junction, Condition(column, operation_type, operator, value))
            self.where_objects.append(where_object)

    def join(self):
        Thread.join(self)
        return self.where_objects

class GroupByParser(Thread):
    def __init__(self, phrases, tables_of_from):
        Thread.__init__(self)
        self.group_by_objects = []
        self.phrases = phrases
        self.tables_of_from = tables_of_from

        self.es_func = ES_Functions()

    def run(self):
        for table_of_from in self.tables_of_from:
            group_by_object = GroupBy()
            for phrase in self.phrases:
                for i in range(0, len(phrase)):
                    if self.es_func.es_search_column(phrase[i]):
                        column = self.es_func.get_column_name_with_alias_table(phrase[i], table_of_from)
                        group_by_object.set_column(column)
            self.group_by_objects.append(group_by_object)

    def join(self):
        Thread.join(self)
        return self.group_by_objects

class OrderByParser(Thread):
    def __init__(self, phrases, tables_of_from):
        Thread.__init__(self)
        self.order_by_objects = []
        self.phrases = phrases
        self.tables_of_from = tables_of_from

        self.es_func = ES_Functions()

    def run(self):
        for table_of_from in self.tables_of_from:
            order_by_object = OrderBy()
            for phrase in self.phrases:
                for i in range(0, len(phrase)):
                    if self.es_func.es_search_column(phrase[i]):
                        column = self.es_func.get_column_name_with_alias_table(phrase[i], table_of_from)
                        order_by_object.add_column(column)
            order_by_object.set_order(0)
            self.order_by_objects.append(order_by_object)

    def join(self):
        Thread.join(self)
        return self.order_by_objects

class Parser:
    database_object = None
    language = None
    thesaurus_object = None

    count_keywords = []
    sum_keywords = []
    average_keywords = []
    max_keywords = []
    min_keywords = []
    junction_keywords = []
    disjunction_keywords = []
    greater_keywords = []
    less_keywords = []
    between_keywords = []
    order_by_keywords = []
    group_by_keywords = []
    negation_keywords = []

    def __init__(self, database, config):
        self.database_object = database

        self.count_keywords = config.get_count_keywords()
        self.sum_keywords = config.get_sum_keywords()
        self.average_keywords = config.get_avg_keywords()
        self.max_keywords = config.get_max_keywords()
        self.min_keywords = config.get_min_keywords()
        self.junction_keywords = config.get_junction_keywords()
        self.disjunction_keywords = config.get_disjunction_keywords()
        self.greater_keywords = config.get_greater_keywords()
        self.less_keywords = config.get_less_keywords()
        self.between_keywords = config.get_between_keywords()
        self.order_by_keywords = config.get_order_by_keywords()
        self.group_by_keywords = config.get_group_by_keywords()
        self.negation_keywords = config.get_negation_keywords()
        self.tagger = PTag("./POSTagger/models/english-bidirectional-distsim.tagger", "./POSTagger/stanford-postagger.jar")

        self.es_func = ES_Functions()

    def set_thesaurus(self, thesaurus):
        self.thesaurus_object = thesaurus

    def remove_accents(self, string):
        nkfd_form = unicodedata.normalize('NFKD', unicode(string))
        return u"".join([c for c in nkfd_form if not unicodedata.combining(c)])

    def posTags(self,s):
        return self.tagger.tag(s.split())

    def parse_sentence(self, sentence):
        number_of_table = 0
        number_of_select_column = 0
        number_of_where_column = 0
        last_table_position = 0
        columns_of_select = []
        columns_of_where = []
        tables_of_from = []
        select_phrase = ''
        from_phrase = ''
        where_phrase = ''
        all_tables_from = []

        words = re.findall(r"[\w]+", self.remove_accents(sentence))

        tags = self.posTags(sentence)
        
        ##############################
        #### Experimental Section ####
        ##############################
        ## The aim is to convert ever-
        ## ything to lower case so th-
        ## at app/lang/english.csv wo-
        ## rks well with the input.
        ##############################
        for i in range(len(words)):
            words[i] = words[i].decode('utf-8').lower()
            tags[i] = (tags[i][0].decode('utf-8').lower(),tags[i][1])
        print "Experimental Debug:", tags, len(tags)
        print "Experimental Debug:", words, len(words)
        ##############################

        '''for (word, tag) in tags:
            if tag in ['NN', 'NNS', 'JJ', 'VB','NNP','NNPS']:
                words.append(word)'''
        for i in range(0, len(words)):
            temp = self.es_func.es_search_column(words[i])
            if temp != None:
                if number_of_table == 0:
                    select_phrase = words[:i]
                tables_of_from.append(self.es_func.es_get_table_from_column(temp))
                all_tables_from += self.es_func.es_get_all_tables(temp)
                number_of_table+=1
                last_table_position = i
                columns_of_select.append(temp)
                number_of_select_column += 1

            if [t for w,t in tags][i] in ['NN', 'NNS', 'JJ', 'VB','NNP','NNPS'] and temp != None:

                if number_of_table == 0:
                    columns_of_select.append(temp)
                    number_of_select_column+=1

                else:
                    if number_of_where_column == 0:
                        from_phrase = words[len(select_phrase):last_table_position + 1]
                        columns_of_where.append(temp)
                        number_of_where_column += 1
            else:
                pass

        where_phrase = words[len(select_phrase) + len(from_phrase):]

        if (number_of_select_column + number_of_table + number_of_where_column) == 0:
            raise ParsingException("No keyword found in sentence!")


        if len(tables_of_from) > 0:
            from_phrases = []
            previous_index = 0
            for i in range(0,len(from_phrase)):
                if self.es_func.es_get_table_from_column(from_phrase[i]) in tables_of_from:
                    from_phrases.append(self.es_func.es_get_table_from_column(from_phrase[i]).encode('utf-8').split())
                    previous_index = i+1
            last_junction_word_index = -1

            for i in range(0, len(from_phrases)):
                number_of_junction_words = 0
                number_of_disjunction_words = 0

                for word in from_phrases[i]:
                    if word in self.junction_keywords:
                        number_of_junction_words += 1
                    if word in self.disjunction_keywords:
                        number_of_disjunction_words += 1

                if (number_of_junction_words + number_of_disjunction_words) > 0:
                    last_junction_word_index = i

            if last_junction_word_index == -1:
                from_phrase = sum(from_phrases[:1], [])
                where_phrase = sum(from_phrases[1:], []) + where_phrase
            else:
                 from_phrase = sum(from_phrases[:last_junction_word_index+1], [])
                 where_phrase = sum(from_phrases[last_junction_word_index+1:], []) + where_phrase

        real_tables_of_from = []

        for word in from_phrase:
            if word in tables_of_from:
                real_tables_of_from.append(word)
        tables_of_from = real_tables_of_from

        if len(tables_of_from) == 0:
            raise ParsingException("No table name found in sentence!")

        group_by_phrase = []
        order_by_phrase = []
        new_where_phrase = []
        previous_index = 0
        previous_phrase_type = 0
        yet_where = 0


        for i in range(0, len(where_phrase)):
            if where_phrase[i] in self.order_by_keywords:
                if yet_where > 0:
                    if previous_phrase_type == 1:
                        order_by_phrase.append(where_phrase[previous_index:i])
                    elif previous_phrase_type == 2:
                        group_by_phrase.append(where_phrase[previous_index:i])
                else:
                    new_where_phrase.append(where_phrase[previous_index:i])
                previous_index = i
                previous_phrase_type = 1
                yet_where += 1
            if where_phrase[i] in self.group_by_keywords:
                if yet_where > 0:
                    if previous_phrase_type == 1:
                        order_by_phrase.append(where_phrase[previous_index:i])
                    elif previous_phrase_type == 2:
                        group_by_phrase.append(where_phrase[previous_index:i])
                else:
                    new_where_phrase.append(where_phrase[previous_index:i])
                previous_index = i
                previous_phrase_type = 2
                yet_where += 1

        if previous_phrase_type == 1:
            order_by_phrase.append(where_phrase[previous_index:])
        elif previous_phrase_type == 2:
            group_by_phrase.append(where_phrase[previous_index:])
        else:
            new_where_phrase.append(where_phrase)

        all_queries, all_real_queries = self.generate_queries(columns_of_select, all_tables_from, select_phrase, columns_of_where, new_where_phrase, group_by_phrase, order_by_phrase)

        # myDB = WikiSQL(columns_of_select, all_tables_from, select_phrase, columns_of_where, new_where_phrase, group_by_phrase, order_by_phrase)
        # myDB.check()
        # columns_of_select, all_tables_from, select_phrase, columns_of_where, new_where_phrase, group_by_phrase, order_by_phrase = myDB.get_real_query_params()

        # all_real_queries = self.generate_queries(columns_of_select, all_tables_from, select_phrase, columns_of_where, new_where_phrase, group_by_phrase, order_by_phrase)
        return (all_queries, all_real_queries)

    def generate_queries(self, columns_of_select, all_tables_from, select_phrase, columns_of_where, new_where_phrase, group_by_phrase, order_by_phrase):
        wiki = WikiSQL()
        all_queries = []
        all_real_queries = []
        for tables_of_from in all_tables_from:
            tables_of_from = [str(tables_of_from)]
            print columns_of_select
            print tables_of_from
            print select_phrase
            print columns_of_where
            print new_where_phrase
            select_parser = SelectParser(columns_of_select, tables_of_from, select_phrase, self.count_keywords, self.sum_keywords, self.average_keywords, self.max_keywords, self.min_keywords)
            from_parser = FromParser(tables_of_from, columns_of_select, columns_of_where, self.database_object)
            where_parser = WhereParser(new_where_phrase, tables_of_from, self.count_keywords, self.sum_keywords, self.average_keywords, self.max_keywords, self.min_keywords, self.greater_keywords, self.less_keywords, self.between_keywords, self.negation_keywords, self.junction_keywords, self.disjunction_keywords)
            group_by_parser = GroupByParser(group_by_phrase, tables_of_from)
            order_by_parser = OrderByParser(order_by_phrase, tables_of_from)

            select_parser.start()
            from_parser.start()
            where_parser.start()
            group_by_parser.start()
            order_by_parser.start()

            queries = from_parser.join()

            if queries is None:
                raise ParsingException("There is at least one unattainable column from the table of FROM!")

            select_objects = select_parser.join()
            where_objects = where_parser.join()
            group_by_objects = group_by_parser.join()
            order_by_objects = order_by_parser.join()
            # print [str(x) for x in queries], tables_of_from
            # print [str(x) for x in select_objects], columns_of_select

            real_queries = []
            for i in range(0, len(queries)):
                query = queries[i]
                query.set_select(select_objects[i])
                query.set_where(where_objects[i])
                query.set_group_by(group_by_objects[i])
                query.set_order_by(order_by_objects[i])

                all_columns = columns_of_select
                #print "column:", all_columns
                real_query = str(query)
                for table in tables_of_from:
                    real_table = wiki.get_real_table_name(table)
                    all_real_columns = wiki.get_real_column_names(table, all_columns)
                    #print "all real column:", all_real_columns
                    if len(all_real_columns) == len(all_columns):
                        for column, real_column in zip(all_columns, all_real_columns):
                            real_query = real_query.replace(table+"."+column, real_table+"."+real_column)
                        real_query = real_query.replace(table, real_table)
                        real_queries.append(real_query)

                        all_queries.append(queries)
                        all_real_queries.append(real_queries)
                    else:
                        continue
                    #break
                #break
            #break
        return all_queries, all_real_queries
