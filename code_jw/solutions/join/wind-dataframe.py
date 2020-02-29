import findspark
findspark.init()
from pyspark.sql import functions as func
from pyspark.sql import Row, SparkSession

datafiles='/home/big/sql/*.csv'

spark = SparkSession.builder.getOrCreate()


# read the wind data from CSV files
df = spark.read.csv(datafiles, header=True, inferSchema=True)

# show the top 5 rows
df.show(5)


#rdd method
winds = df.rdd

mapped = winds.map(lambda s: (s.Station_ID, s.Wind_Velocity_Mtr_Sec))

maxes = mapped.reduceByKey(lambda a,b: a if (a>b) else b)
for (k,v) in maxes.collect() :
    print k,v

print maxes.collectAsMap()


#DataFrame method
df.groupBy('Station_ID').agg(func.avg('Wind_Velocity_Mtr_Sec').alias("avg"), func.max('Wind_Velocity_Mtr_Sec').alias("max)")).show()


#SQL method
df.registerTempTable('wind')
spark.sql("SELECT Station_ID, AVG(Wind_Velocity_Mtr_Sec) AS avg, MAX(Wind_Velocity_Mtr_Sec) AS max FROM wind GROUP BY Station_ID").show()



