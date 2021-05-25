#!/usr/bin/env python
# coding: utf-8

# In[1]:


import copy
import findspark
findspark.init()


# In[2]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
sc=spark.sparkContext


# In[76]:


# df = spark.read.format("csv").option("header", "true").load('./data/2018-01_bme280sof.csv')
df = spark.read.format("csv").option("header", "true").load('./data/ToyTable.csv')


# In[77]:


df = df.drop('_c0')


# In[78]:


# df.createOrReplaceTempView("sofia")
df.createOrReplaceTempView("toy")


# In[142]:


# small_df = spark.sql("select * from sofia limit 1000")
toy_df = spark.sql("select * from toy")


# In[143]:


from operator import add
# schema = small_df.columns
schema = toy_df.columns
schema


# In[144]:


LHS = {'I1'}
RHS = 'S2'


# In[145]:


zeroVal1 = ([], 0)
mergeVal1 = (lambda aggregated, el: (aggregated[0] + [el[0]], aggregated[1] + el[1]))    
mergeComb1 = (lambda agg1,agg2:agg1+agg2)


# In[146]:


zeroVal2 = ([], [], [])
mergeVal2 = (lambda aggregated, el: (aggregated[0] + [el[0]], aggregated[1] + [el[1]], aggregated[2] + [el[2]]))    
mergeComb2 = (lambda agg1,agg2:agg1+agg2)


# In[148]:


# map and reduce into a five-tuple rdd
rdd1 = toy_df.rdd.map(lambda x: (*LHS, RHS, tuple(x[idx] for idx in list(map(lambda y: schema.index(y),LHS))), x[schema.index(RHS)])).map(lambda tpe: (tpe,1)).reduceByKey(add)
# remap five-tuple key and combine by X,A key
rdd2 = rdd1.map(lambda x: ((x[0][:-1]), ((x[0][-1], x[1]), x[1]))).aggregateByKey(zeroVal1,mergeVal1,mergeComb1).map(lambda x: ((x[0][:-1]), (x[0][-1], *x[1]))).aggregateByKey(zeroVal2,mergeVal2,mergeComb2).collect()
for line in rdd2:
    print(line)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




