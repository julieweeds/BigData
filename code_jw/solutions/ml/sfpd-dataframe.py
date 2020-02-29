import findspark
findspark.init()

spark = SparkSession.builder.getOrCreate()
from dateutil.parser import parse
from datetime import datetime
from pyspark.sql.functions import udf, col, year
from pyspark.sql.types import TimestampType

import pyspark.sql.functions as func
from scipy import spatial
from numpy import array
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics

#process weather averages
def date_and_hour(s):
    dt = parse(s.replace('?',' '))
    return dt

date_and_hour_udf = udf(date_and_hour, TimestampType())

weather = spark.read.csv('/home/big/join/weather/*.csv', header = True, inferSchema=True)#.limit(100)

cleaned = weather.filter(weather.Wind_Velocity_Mtr_Sec != 0).drop("Station_Name", "Location_Label", "Interval_Minutes", "Wind_Direction_Variance_Deg", "Global_Horizontal_Irradiance")

cleaned = cleaned.withColumn("date_time_hour", date_and_hour_udf("Interval_End_Time"))


byIdTime = Window.partitionBy("Station_ID", "date_time_hour")

avg = cleaned.groupBy("Station_ID", "date_time_hour").agg( func.avg("Wind_Velocity_Mtr_Sec").alias("avg_wind"), func.avg("Ambient_Temperature_Deg_C").alias("avg_temp") )
avg = avg.cache()
avg.show()


#process incidents to closest weather stations
incidents = spark.read.csv('/home/big/join/incidents/*.csv', header = True)

def date_and_hour2(d, h):
    s = d + " " + h
    dt = parse(s)
    return dt

date_and_hour2_udf = udf(date_and_hour2 ,TimestampType())

incidents = incidents.withColumn("date_time_hour", date_and_hour2_udf(col("Date"), col("Time")))
incidents = incidents.drop("Category","Descript","DayOfWeek", "Date", "Time", "PdDistrict", "Resolution", "Address", "Location", "PdId")
incidents = incidents.filter( (year(col('date_time_hour')) == '2014') & (col("X") > -130) & (col("X") < -120) & (col("Y") > 30) & (col("Y") < 40 ) )
incidents = incidents.cache()
incidents.show()


latlongs=[[37.7816834,-122.3887657],[37.7469112,-122.4821759],[37.7411022,-120.804151],[37.4834543,-122.3187302],[37.7576436,-122.3916382],[37.7970013,-122.4140409],[37.748496,-122.4567461],[37.7288155,-122.4210133],[37.5839487,-121.9499339],[37.7157156,-122.4145311],[37.7329613,-122.5051491],[37.7575891,-122.3923824],[37.7521169,-122.4497687]]

locations = ["SF18", "SF04", "SF15", "SF17", "SF36", "SF37", "SF07", "SF11", "SF12", "SF14", "SF16", "SF19", "SF34"]

def locate(l,index,locations):
    distance,i=index.query(l)
    return locations[i]

locate_udf = udf(lambda x, y : locate([float(x),float(y)], spatial.KDTree(array(latlongs)),locations))

incidents = incidents.withColumn("loc", locate_udf(col("X"), col("Y")) ).cache()

incident_counts = incidents.groupBy(["date_time_hour", "loc"]).count()
incident_counts.show()

joined = incident_counts.join(avg, [incident_counts.loc == avg.Station_ID, incident_counts.date_time_hour == avg.date_time_hour])
joined.show()



#remap the data into a Vector of [t, w, i]
vecs = joined.rdd.map(lambda row: Vectors.dense([row['avg_temp'],row['avg_wind'],row['count']]))
print(Statistics.corr(vecs))

