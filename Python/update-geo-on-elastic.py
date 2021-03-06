from elasticsearch import Elasticsearch

import time
import datetime
import editdistance
import json

import pandas as pd


ES_INDEX = 'poi'
ES_TYPE = 'poi_elastic'
FILE_PATH = "/home/paliwal/geo/poi/geo.json"

es = Elasticsearch([{'host': 'xxx.xxx.xxx.xx', 'port': xxxx}])

def query(coor):
  res = es.search(index=ES_INDEX, doc_type=ES_TYPE, body={
    "query": {
      "bool" : {
        "must" : {
          "match_all" : {}
        },
        "filter" : {
          "geo_distance" : {
            "distance" : "0.1km",
            "location" : coor
          }
        }
      }
    }
  })
  
  return res
  
def load_json(file_path):
  with open(file_path) as json_file:
    json_data = json.load(json_file)
  
  return json_data


if __name__ == '__main__':
  
  print("Loading json")
  json_data = load_json(FILE_PATH)

  settings = {
      "settings": {
          "number_of_shards": 1,
          "number_of_replicas": 0
      },
      "mappings": {
      "poi_elastic": {
        "properties": {
          "location": {
            "type": "geo_point"
          },
          "cat": {
            "type": "string"
          },
          "poi_name": {
            "type": "string"
          },
          "address": {
            "type": "string"
          },
          "source": {
            "type": "string"
          }
        }
      }
    }
  }
  
  print("Pushing/updating data in progress...")
  total_pushed = 0
  total_duplicates_unpushed = 0
  for row in json_data:
    if ('poi_name' in row):
      npoi=''.join((row['poi_name']).split()) #resolve whitespace 
    else:
      npoi=''
    doc = {
       'location': row['location'],
       'cat': row['cat'],
       'poi_name': npoi ,
       'address': row['address'],
       'source': row['source'],
       'timestamp': '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
    }
    potential_duplicates = query(row['location'])
    
    #Levenshtein distance based poi dulicate matching
    if(potential_duplicates['hits']['total']==0):
      total_pushed = total_pushed + 1
      es.index(index=ES_INDEX, doc_type=ES_TYPE, body=doc)
    else:
      i=0
      not_matched=0
      for potential_duplicate in potential_duplicates['hits']['hits']:
        i=i+1
        s1 = npoi  
        s2 = ''.join((potential_duplicate['_source']['poi_name']).split()) 
      
        if(editdistance.eval(s1.lower(),s2.lower()) <=1):
          total_duplicates_unpushed = total_duplicates_unpushed + 1
          es.update(index=ES_INDEX, doc_type=ES_TYPE,id=potential_duplicate['_id'],
                body= { "doc": 
                  {'location': row['location'],'cat': row['cat'],
                  'poi_name': npoi ,'address': row['address'],
                  'source': row['source'],'timestamp': '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
                  }
                })
        else:
          not_matched=not_matched+1
      
      if(i==not_matched):
        total_pushed = total_pushed + 1
        es.index(index=ES_INDEX, doc_type=ES_TYPE, body=doc)
        
  f = open('/home/paliwal/geo/geo_database.txt', 'wb')
  dict = {'total_pushed': total_pushed, 'total_duplicates_unpushed)': total_duplicates_unpushed, 'Status': "Finished."}
  f.write('dict = ' + repr(dict) + '\n' )
  f.close()

