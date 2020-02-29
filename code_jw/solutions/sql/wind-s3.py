# spark initialisation, needs to be done before later imports
#import findspark
#findspark.init()
import pyspark
sc=pyspark.SparkContext.getOrCreate()
sc.setLogLevel('ERROR')

#where the data is stored
datafiles='s3a://discnet-big/wind2015/*.csv'

#initialisation of spark sql session
from pyspark.sql import Row, SparkSession
spark = SparkSession.builder.getOrCreate()

# read the wind data from CSV files
df = spark.read.csv(datafiles, header=True)

df.registerTempTable('wind')
spark.sql("SELECT Station_ID, avg(Wind_Velocity_Mtr_Sec) as avg,max(Wind_Velocity_Mtr_Sec) as max from wind group by Station_ID").show()