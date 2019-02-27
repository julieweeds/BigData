from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession

from dateutil.parser import parse
from datetime import datetime


datafile='/home/big/join/weather/*.csv'  #for all files
datafile='/home/big/join/weather/SF04.csv'  #to test on just one file

###load data using SparkSession
spark=SparkSession.builder.getOrCreate()
df=spark.read.csv(datafile,header=True)

### alternative way of loading using SQLContext
#conf = SparkConf().setAppName("wind-sfpd")
#sc = SparkContext(conf=conf)
#sqlc = SQLContext(sc)


#df = sqlc.read.format('com.databricks.spark.csv').\
#options(header='true', inferschema='true').\
#load(datafile)

def date_and_hour(s):
    dt = parse(s.replace('?',' '))
    hour = dt.hour
    return (dt.strftime("%Y-%m-%d"), hour)

#just get the fields of interest and parse the date and hour from the Interval_End_Time
tidied = df.rdd.map(lambda r: (r.Station_ID, date_and_hour(r.Interval_End_Time), \
(r.Ambient_Temperature_Deg_C, r.Wind_Velocity_Mtr_Sec)))

#restructure columns so that key is made up of (s,d,h) and value is (t,w)
tidier = tidied.map(lambda (s, (d,h), (t,w)): ((s,d,h),(t,w)))

#filter out zero and null values
filtered = tidier.filter( lambda ((s,d,h),(t,w)):  not (t==0.0 and w==0.0))

filter2 = filtered.filter(lambda ((s,d,h),(t,w)):  bool(t) and  bool(w))

#add a column of 1s which will be used for counting
mapped = filter2.map( lambda ((s,d,h),(t,w)): ((s,d,h), (t,w,1)))

#group by key and sum all of the values (t,w,c)
reduced = mapped.reduceByKey( lambda (t1,w1,c1), (t2,w2,c2): (t1+t2,w1+w2,c1+c2))

#calculate averages
averaged = reduced.map(lambda ((s,d,h), (t,w,c)): ((s,d,h, t/c, w/c)))

#output results
for (key,value) in averaged.sortByKey().collect():
	print(key,value)


 

