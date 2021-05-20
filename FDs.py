from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import *

def helper(month):
 df = spark.read.csv(path='hdfs:///group/mcs-2amd15-19/data/2018-'+month+'_bme280sof.csv', sep=',', encoding='UTF-8', comment=None, header=True, inferSchema=True)

 final_data = df.na.drop().withColumn("month", lit(month)).withColumn('lat_tmp', format_number(df.lat, 3)).withColumn('lon_tmp', format_number(df.lon, 3)).drop('_c0','lat','lon').withColumnRenamed('lat_tmp','lat').withColumnRenamed('lon_tmp','lon')

 return final_data

def find_FDs(df):

 fields = df.columns
	
 FDs = []
	
 df.createOrReplaceTempView("air")

 for i in range(0, len(fields)):
     for j in range(0, len(fields)):
         #same column skip compare
         if (i == j): 
            continue
			
         field_1 = fields[i]
         field_2 = fields[j]
         res = spark.sql(f'SELECT {field_1}, COUNT(DISTINCT {field_2}) c FROM air GROUP BY {field_1} HAVING c > 1').count()

         if (res == 0):
            FDs.append(f'{field_1} -> {field_2}')

 for i in range(0, len(fields)):
     for j in range(0, len(fields)):
         for k in range(0, len(fields)):
             #same column skip compare
             if (i == k or j==k or j<=i):
                 continue

             field_1 = fields[i]
             field_2 = fields[j]
             field_3 = fields[k]
             res = spark.sql(f'SELECT {field_1}, {field_2}, COUNT(DISTINCT {field_3}) c FROM air GROUP BY {field_1}, {field_2} HAVING c > 1').count()

             if (res == 0):
                 FDs.append(f'{field_1},{field_2} -> {field_3}')

 for i in range(0, len(fields)):
     for j in range(0, len(fields)):
         for k in range(0, len(fields)):
             for p in range(0, len(fields)):
                 #same column skip compare
                 if (j<=i or i == p or k<=j or j==p or k==p):
                     continue

                 field_1 = fields[i]
                 field_2 = fields[j]
                 field_3 = fields[k]
                 field_4 = fields[p]
                 res = spark.sql(f'SELECT {field_1}, {field_2}, {field_3}, COUNT(DISTINCT {field_4}) c FROM air GROUP BY {field_1}, {field_2}, {field_3} HAVING c > 1').count()

                 if (res == 0):
                     FDs.append(f'{field_1},{field_2},{field_3} -> {field_4}')
    

 if len(FDs)>0:
    print('Following functional dependencies found:')
    for fd in FDs:
        print(fd)
 else:
    print('No functional dependencies found.')


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

# res = month_1.union(month_2)

 find_FDs(month_1)


 
 

