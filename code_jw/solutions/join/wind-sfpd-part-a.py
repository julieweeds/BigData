# spark initialisation, needs to be done before later imports
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession

from dateutil.parser import parse
from datetime import datetime


datafile='weather/*.csv'  #for all files
datafile='weather/SF04.csv'  #to test on just one file

### can load data using SparkSession like this but then will have to explicitly handle format conversions
#spark=SparkSession.builder.getOrCreate()
#df=spark.read.csv(datafile,header=True)

### better to load data using SQLContext and csv schema (which deals with formatting)
conf = SparkConf().setAppName("wind-sfpd")
sc = SparkContext(conf=conf)
sqlc = SQLContext(sc)


df = sqlc.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(datafile)



def date_and_hour(s):
    dt = parse(s.replace('?',' '))
    hour = dt.hour
    return (dt.strftime("%Y-%m-%d"), hour)

#df.show()

stations=df.rdd
#just get the fields of interest and parse the date and hour from the Interval_End_Time
tidied = stations.map(lambda r: (r.Station_ID, date_and_hour(r.Interval_End_Time)[0],date_and_hour(r.Interval_End_Time)[1],r.Ambient_Temperature_Deg_C, r.Wind_Velocity_Mtr_Sec))
#(s,d,h,t,w)
#filter out zero and null values
filtered = tidied.filter( lambda r:  not (r[3]==0.0 and r[4]==0.0))

filter2 = filtered.filter(lambda r:  bool(r[3]) and  bool(r[4]))

#group first three columns as key and last two columns as value
#add a column of 1s which will be used for counting
mapped = filter2.map( lambda r: ((r[0],r[1],r[2]),(r[3],r[4],1)))

#group by key and sum all of the values (t,w,c)
reduced = mapped.reduceByKey( lambda a, b: (a[0]+b[0],a[1]+b[1],a[2]+b[2]))

#calculate averages
averaged = reduced.map(lambda x: (x[0], (x[1][0]/x[1][2], x[1][1]/x[1][2])))

#output results
for (key,value) in averaged.sortByKey().collect():
	print(key,value)

