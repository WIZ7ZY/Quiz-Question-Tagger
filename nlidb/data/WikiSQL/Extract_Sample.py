import json

# Read all the tables from dev 
f2 = open('C:/New Software/WikiSQL/data/dev.tables.jsonl', 'r')
dataset =[]
tables=[]
extractedData=[]
for line in f2.readlines():
    try:
        data = json.loads(line)
        dataset.append(data)
    except:
        pass


for data in dataset:
    table = data['id']
    tables.append(table)
tables = tables[:100]

#Load the questions dev
f = open('C:/New Software/WikiSQL/data/dev.jsonl', 'r')
for line in f.readlines():
    data = json.loads(line)
    try:
        data = json.loads(line)
        if data['table_id'] in tables:
            extractedData.append(data)
    except:
        pass

with open("testfile.jsonl","a") as thefile:
    for item in extractedData:
        json.dump(item, thefile)
        thefile.write("\n")
        print item

with open("testfile.tables.jsonl","a") as ft:
    for t in dataset[:100]:
        json.dump(t, ft)
        ft.write("\n")