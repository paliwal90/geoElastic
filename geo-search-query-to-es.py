from elasticsearch import Elasticsearch

import time
import pandas as pd

ES_INDEX = 'poi'
ES_TYPE = 'poi_elastic'
FILE_PATH = "/home/paliwal/geo/poi/sampled_locations.csv"

es = Elasticsearch([{'host': 'xxx.xxx.xxx.xx', 'port': xxxx}])

def query(coor, start_time):
  res = es.search(index=ES_INDEX, doc_type=ES_TYPE, body={
    "query": {
          "bool" : {
              "must" : {
                  "match_all" : {}
              },
              "filter" : {
                  "geo_distance" : {
                      "distance" : "5km",
                      "location" : coor
                  }
              }
          }
      }
  })
  
  #change it to retrive whatever desired content
  #for doc in res['hits']['hits']:
    #print(doc)
  print("%d documents" % res['hits']['total'])

  query_time = time.time() - start_time
  print("--- %s milliseconds ---" % int(round(query_time * 1000))) 
  
def read_sampled_loc(file_path):  
  coors = pd.read_csv(file_path, sep="\t") #to not sep by ',' due to entries(locations) having ','
  return coors


if __name__ == '__main__':
  
  coors = read_sampled_loc(FILE_PATH)

  for coor in coors.ix[:,0]:
    start_time = time.time()
    print(coor)
    query(coor, start_time)
  


