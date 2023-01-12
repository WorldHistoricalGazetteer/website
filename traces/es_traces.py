import sys, codecs, os, json
from django.conf import settings
from elasticsearch7 import Elasticsearch
es = settings.ES_CONN
def index_traces():
  idx = 'traces03'
  wd = '/Users/karlg/Documents/Repos/_whgazetteer/es/'
  #file = wd+'trace_data/traces_20200702.json'
  file = wd+'trace_data/traces_20200722_all.json'
  # read from file 
  infile = codecs.open(file, 'r', 'utf8')
  trdata = json.loads(infile.read())
  
  print('data is:',type(trdata))
  count=0
  for rec in trdata[1:]:
    print(rec['id'])
    try:
      del rec['@context'] # not needed in index
      es.index(index=idx, doc_type='trace',body=rec)
      count +=1
    except:
      print(rec['id'], ' broke it')
      print("error:", sys.exc_info())
  print(str(count)+' records indexed')

def init():
  global es, idx
  from elasticsearch import Elasticsearch
  es = settings.ES_CONN
  idx = 'traces03'
  wd = '/Users/karlg/Documents/Repos/_whgazetteer/es/'  
  mappings = codecs.open(wd+'mappings_traces03.json', 'r', 'utf8').read()

  from elasticsearch import Elasticsearch
  es = settings.ES_CONN
  # zap existing if exists, re-create
  try:
    es.indices.delete(idx)
  except Exception as ex:
    print(ex)
  try:
    es.indices.create(index=idx, ignore=400, body=mappings)
    #es.indices.create(index=idx, ignore=400)
    print ('index "'+idx+'" created')
  except Exception as ex:
    print(ex)

# re-formats early data (Xuanzang, etc)
def reorg_traces():
  global es, idx, rows, trdata
  wd = '/Users/karlg/Documents/Repos/_whgazetteer/es/'
  idx = 'traces03'
  file = wd+'trace_data/examples_whg_initial.json'
  import os, codecs, time, json

  es = settings.ES_CONN
  # read from file 
  fn = 'trace_data/traces_20200722.json'
  infile = codecs.open(file, 'r', 'utf8')
  fout = codecs.open(wd+fn, 'w', 'utf8')
  olddata = json.loads(infile.read())
  newdata = []
  for d in olddata:
    target = d['target']
    oldbodies = d['body']
    newbodies = []
    
    # add to target
    d['target']["format"] = "text/html"
    d['target']["language"] = "en"
    d['target']["type"] = target["type"][0]
  
    for b in oldbodies:
      print(b)
      new_body = {"id":b["id"], 
                  "title":b["title"], 
                  "place_id":b["place_id"], 
                  "relations":[
                    {"when":b["when"] if "when" in b else [],
                     "relation":b["relation"]}
                  ]}
      newbodies.append(new_body)
    d["body"] = newbodies
    d["tags"] = []
    d["tags_suggest"] = []
    
    newdata.append(d)
  fout.write(json.dumps(newdata,indent=2))
  fout.close()

