from pyspark import SparkContext
import csv
import sys
import fiona
import fiona.crs
import shapely

def extractData(_, lines):
    import re
    import rtree
    import pyproj
    import shapely.geometry as geom   
    
    # Predefine some useful variables for processing
    separator = re.compile('\W+')
    proj = pyproj.Proj(init="epsg:5070", preserve_units=True)
    counts = {}
    
#    index = rtree.Rtree()
#    for idx,geometry in enumerate(tracts.geometry):
#        index.insert(idx, geometry.bounds)

    for line in lines:
        data = line.split('|')
        # Split into Coords and Tweets 
        # NB Need to decide to use 5 or 6/-1,-2?????????
        lat, lon, text, act = float(data[1]), float(data[2]), data[-1], data[-2]
        textset = separator.split(text.lower())
        
        # Split words from terms into each item to compare multi-word items
        for word in terms:
            # Ensure that multi-word items are accounted for (multiple matches)
            if len(set(word.split()).intersection(textset))>=len(word.split()):
                # Define point of long and lat
                p = geom.Point(proj(lon, lat))
                
                # Define variables
                match = None
                tractid = 0
                
                # Find the exact polygon that intersects
                for idx in index.intersection((p.x, p.y, p.x, p.y)):
                    shape = tracts.geometry[idx]
                
                    # ensure shape as defined as polygon includes coords
                    if shape.contains(p):
                        # Define detail as per intersecting geometry
                        match = idx
                        tractid = tracts.plctract10[idx]
                        pop = tracts.plctrpop10[idx]
                        break
                # Add Matching data to counts
                if match:
                    counts[tractid] = counts.get(tractid, 0) + (1/pop)
    return list(counts.items())



if __name__=='__main__':
    sc = SparkContext()
    
    # Input spark-submit --executor-cores 5 --num-executors 10 --py-files … \ 
    # --files hdfs:///tmp/500cities_tracts.geojson,hdfs:///tmp/drug_sched2.txt,hdfs:///tmp/drug_illegal.txt \ 
    # final_challenge.py hdfs:///tmp/tweets-100m.csv final_challenge_output.csv
    
    # Source Files 
    DATA = ''
    TweetsSource = DATA + sys.argv[-2]
    Drugs1Source = DATA + sys.argv[-3]
    Drugs2Source = DATA + sys.argv[-4]
    shapefile = DATA + sys.argv[-5]
    
    # Define gemetry and convert coordinates to correct format
    tracts = gpd.read_file(shapefile )
    tracts.crs = fiona.crs.from_epsg(4326)
    tracts = tracts.to_crs(fiona.crs.from_epsg(5070))

    # Create Rtree
    index = rtree.Rtree()
    for idx,geometry in enumerate(tracts.geometry):
        index.insert(idx, geometry.bounds)

    # Combine terms of drug types into list
    terms = set(map(lambda x: x.strip(), (open(Drugs1Source, 'r').readlines() + 
                                          open(Drugs2Source, 'r').readlines())))

    
    
    
    
    tweets = sc.textFile(TweetsSource, use_unicode=True).cache()\
                .mapPartitionsWithIndex(extractData)\
                .reduceByKey(lambda x,y: x+y)\
                .sortBy(lambda x: -x[1])\
                .saveAsTextFile(sys.argv[-1])