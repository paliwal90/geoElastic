from elasticsearch import Elasticsearch
from pyelasticsearch import ElasticSearch 
from pyelasticsearch import bulk_chunks

import json

ES_INDEX = 'poi'
ES_TYPE = 'poi_elastic'
FILE_PATH = "/home/paliwal/geo/poi/geo.json"

# LoopMe production ElasticSearch credintials
es = Elasticsearch([{'host': 'xxx.xxx.xxx.xx', 'port': xxxx}])
es2 = ElasticSearch('http://xxx.xxx.xxx.xx:xxxx/')

def load_json(file_path):
  with open(file_path) as json_file:
    json_data = json.load(json_file)
  
  return json_data


def documents(json_data):
    for doc in json_data:
        yield es2.index_op(doc)

if __name__ == '__main__':
  
  if es.indices.exists(ES_INDEX):
    print("deleting '%s' index..." % (ES_INDEX))
    res = es.indices.delete(index = ES_INDEX)
    print(" response: '%s'" % (res))

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

  print("creating '%s' index..." % (ES_INDEX))
  res = es.indices.create(index = ES_INDEX,ignore=400, body=settings)
  print(" response: '%s'" % (res))

  print("Loading json...")
  json_data = load_json(FILE_PATH)

  print("Bulk chunk indexing...")
  for chunk in bulk_chunks(documents(json_data),
                           docs_per_chunk=500,
                           bytes_per_chunk=10000):
      es2.bulk(chunk, index=ES_INDEX, doc_type=ES_TYPE)
      

