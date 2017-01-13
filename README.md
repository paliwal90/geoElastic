Extract and Implement GeoSpatial Data to Production ElasticSearch 5
=========================

1. Data Sources
============

Three sources have been used to extra data:

* OSM (Open Street Map) https://github.com/MorbZ/OsmPoisPbf used to convert data from .pdf to .csv.

* Poi Factory http://www.poi-factory.com

* PocketGPS http://www.pocketgpsworld.com/

The following features are stored from each source:

* lat: latitude.
* lon: longitude.
* cat: category of poi_name.
* poi_name: name of poi - point of interest.
* address: address of poi.
* source: source from where data ie collected.


2. OSM pbf to csv conversion
=============================

Tool: https://github.com/MorbZ/OsmPoisPbf

Original filters.txt file was modified that contains unique id to each category.

```
java -Xmx4g -jar osmpois.jar -ff filters_modified.txt planet-latest.osm.pbf
```

This returns planet-latest.csv

3. Clean planet-latest.csv
===========================

[clean-osm-planet-data.R](https://github.com/paliwal90/geoElastic/blob/master/clean-osm-planet-data.R)

4. Combine data from source
===========================

[combine-geo-from-sources.R](https://github.com/paliwal90/geoElastic/blob/master/combine-geo-from-sources.R)


5. Bulk Chunk Indexing to Production ES
=======================================

[geo-bulk-chunk-indexing-to-es.py](https://github.com/paliwal90/geoElastic/blob/master/geo-bulk-chunk-indexing-to-es.py)

6. Geo Search Query to Production ES
=======================================

[geo-search-query-to-es.py](https://github.com/paliwal90/geoElastic/blob/master/geo-search-query-to-es.py)


6. Update Schema for Data on ES
=======================================

[geo-search-query-to-es.py](https://github.com/paliwal90/geoElastic/blob/master/geo-search-query-to-es.py)

Principal Logic for Pushing Data
================================

1. If we know there are no duplicates in locations data, [bulk-bhunk-indexing](https://github.com/paliwal90/geoElastic/blob/master/geo-bulk-chunk-indexing-to-es.py) can be used for effectively pushing data to elastic in chunking.

2. If we suspect (which is true by default) there are duplicates, then for every input location, the following [update-schema](https://github.com/paliwal90/geoElastic/blob/master/update_geo_on_elastic.py) should be supplied.

Principal logic is as follow:

* For each input_poi:
    * Query elastic and retrieve nearby_pois (100metre):
       * If there are no nearby_pois or no data yet in elastic, push input_poi to index.
       * If there are nearby_pois, then:
            * For each nearby_poi, call editdistance(lowered_case_input_poi,lowered_case_nearby_poi):
                * If editdistance is <=1, then they are considered duplicated and corresponding nearby_poi on elastic is updated by input_poi.
            * If editdistance is > 1 for all nearby_pois to input_poi, it means the input_poi is new and hence simply pushed to index.

Reference: [Levenshtein_distance](https://en.wikipedia.org/wiki/Levenshtein_distance)

