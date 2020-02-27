# strip removes any non-alpha characters
def strip(s): return ''.join(filter(str.isalpha, s))

import findspark
findspark.init()

import pyspark
sc = pyspark.SparkContext.getOrCreate()
books = sc.textFile("*.txt")

tokens = books.flatMap(lambda line: line.split())
stripped = tokens.map(strip)
notempty = stripped.filter(lambda w: len(w)>0) #filter any zero length tokens

lower = notempty.map(lambda w: w.lower())  #lowercase everything
mapped = lower.map(lambda w: (w,1))        #map every token to a (token,1) key-pair
wordcount = mapped.reduceByKey(lambda x,y: x+y)   #apply add to accumulate counts for each key

reorder = wordcount.map(lambda p:(p[1],p[0]))   #swap the arguments in the tuples
sort = reorder.sortByKey(False)    #sort by frequency (inverse)

for k,v in sort.collect():
    print (k,v)
