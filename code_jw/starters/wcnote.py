# strip removes any non-alpha characters
def strip(s): return ''.join(filter(str.isalpha, s))

import findspark
findspark.init()

import pyspark
sc = pyspark.SparkContext.getOrCreate()
books = sc.textFile("*.txt")  #you may need to change this depending on your directory structure

tokens = books.flatMap(lambda line: line.split())
stripped = tokens.map(strip)
notempty = stripped.filter(lambda w: len(w)>0)

#now map the words to lower case

#next convert the words into (k,v) pairs, where the key is the word, and the value is the.count so far

#next reduce by key, adding up the counts as you go

#make sure your final variable is called wordcount, so this next line will print it out

for k,v in sort.collect():
    print(k,v)
