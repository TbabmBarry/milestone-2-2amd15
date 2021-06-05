from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import *

def helper(month):
 df = spark.read.csv(path='hdfs:///group/mcs-2amd15-19/data/2018-'+month+'_bme280sof.csv', sep=',', encoding='UTF-8', comment=None, header=True, inferSchema=True)

 final_data = df.na.drop().withColumn("month", lit(month)).withColumn('lat_tmp', format_number(df.lat, 3)).withColumn('lon_tmp', format_number(df.lon, 3)).drop('_c0','lat','lon').withColumnRenamed('lat_tmp','lat').withColumnRenamed('lon_tmp','lon')

 return final_data


if __name__ == '__main__':

 spark = SparkSession.builder.getOrCreate()

 sc = spark.sparkContext

 df = spark.read.csv(path='hdfs:///group/mcs-2amd15-19/data/2018-01_bme280sof.csv', sep=',', encoding='UTF-8', comment=None, header=True, inferSchema=True)

 #rdd = sc.textFile("hdfs:///group/mcs-2amd15-19/data/2018-01_bme280sof.csv") 
 
 month_1 = helper('01')
# month_1.show()
# month_2 = helper('02')
# month_3 = helper('03')
# month_4 = helper('04')
# month_5 = helper('05')
# month_6 = helper('06')
# month_7 = helper('07')
# month_8 = helper('08')
# month_9 = helper('09')
# month_10 = helper('10')
# month_11 = helper('11')
# month_12 = helper('12')


 
 

