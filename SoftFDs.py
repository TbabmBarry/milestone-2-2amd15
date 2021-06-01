#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import findspark
findspark.init()


# In[2]:


import pyspark as ps
from pyspark.sql import SparkSession
conf = ps.SparkConf().setMaster('local').setAppName("softFD")
sc = ps.SparkContext('local[4]', '', conf=conf) # uses 4 cores on your local machine
spark=SparkSession.builder.config(conf=conf).getOrCreate()


# In[3]:


# df = spark.read.format("csv").option("header", "true").load('./data/2018-01_bme280sof.csv')
df = spark.read.format("csv").option("header", "true").load('./data/ToyTable.csv')


# In[4]:


df = df.drop('_c0')


# In[5]:


# df.createOrReplaceTempView("sofia")
df.createOrReplaceTempView("toy")


# In[6]:


# small_df = spark.sql("select * from sofia limit 1000")
toy_df = spark.sql("select * from toy")


# In[7]:


from operator import add
from functools import reduce
# schema = small_df.columns
schema = toy_df.columns
schema


# In[8]:


LHS = {'I1'}
RHS = 'S2'


# In[9]:


zeroVal1 = ([], 0)
mergeVal1 = (lambda aggregated, el: (aggregated[0] + [el[0]], aggregated[1] + el[1]))    
mergeComb1 = (lambda agg1,agg2:agg1+agg2)


# In[10]:


zeroVal2 = ([], [], [])
mergeVal2 = (lambda aggregated, el: (aggregated[0] + [el[0]], aggregated[1] + [el[1]], aggregated[2] + [el[2]]))    
mergeComb2 = (lambda agg1,agg2:agg1+agg2)


# In[11]:


zeroVal3 = ([], [])
mergeVal3 = (lambda aggregated, el: (aggregated[0] + [el[0]], aggregated[1] + [el[1]]))    
mergeComb3 = (lambda agg1,agg2:agg1+agg2)


# In[12]:


# map and reduce into a five-tuple rdd
rdd1 = toy_df.rdd.map(lambda x: (*LHS, RHS, tuple(x[idx] for idx in list(map(lambda y: schema.index(y),LHS))), x[schema.index(RHS)]))    .map(lambda tpe: (tpe,1)).reduceByKey(add)     .map(lambda x: ((x[0][:-1]), ((x[0][-1], x[1]), x[1])))    .aggregateByKey(zeroVal1,mergeVal1,mergeComb1).map(lambda x: ((x[0]), list(map(lambda r: round(r[1] / x[1][1], 3),x[1][0]))))    .map(lambda x: ((x[0]), 1 - reduce(lambda p1, p2:p1+p2,list(map(lambda p: p*(1-p), x[1])))))    .map(lambda x: ((x[0][:-1]), (x[0][-1], x[1]))).aggregateByKey(zeroVal3,mergeVal3,mergeComb3).filter(lambda tup: all(t >= 0.8 for t in tup[1][1]))    .map(lambda tup: tup[0]).collect()
    
# rdd2 = rdd1.map(lambda x: ((x[0][:-1]), ((x[0][-1], x[1]), x[1]))).aggregateByKey(zeroVal1,mergeVal1,mergeComb1).map(lambda x: ((x[0]), list(map(lambda r: round(r[1] / x[1][1], 3),x[1][0])))).collect()
# rdd2 = rdd1.map(lambda x: ((x[0][:-1]), ((x[0][-1], x[1]), x[1]))).aggregateByKey(zeroVal1,mergeVal1,mergeComb1).map(lambda x: ((x[0]), (list(map(lambda r: (r[0], round(r[1] / x[1][1], 3)),x[1][0])), x[1][1]))).collect()
# rdd2 = rdd1.map(lambda x: ((x[0][:-1]), ((x[0][-1], x[1]), x[1]))).aggregateByKey(zeroVal1,mergeVal1,mergeComb1).map(lambda x: ((x[0][:-1]), (x[0][-1], *x[1]))).aggregateByKey(zeroVal2,mergeVal2,mergeComb2).collect()
for line in rdd1:
    print(line)


# In[13]:


def generate_computational_graph(RHS, schema):
    """
    Output
    ----------
    A dictionary where
    key: level
    value: list of current level's candidates, candidates are in the format of set
    -----

    """
    computational_graph=dict()
    for level in range(3):
        #use brute force to generate candidates for each level
        computational_graph[level]=[]
        if level== 0:
            for attribute  in schema:
                if attribute !=RHS:
                    computational_graph[level].append(set([attribute]))

        else:
            for element1 in computational_graph[level-1]:
                for element2 in computational_graph[0]:
                    newelement = element1.union(element2)
                    if newelement not in computational_graph[level]:
                        if len(newelement)==level+1:
                            computational_graph[level].append(newelement)    

    return computational_graph
def get_candidates(level, computational_graph):
    return computational_graph[level]
def prune_graph(level,current_level_result,computational_graph):
    """
    Input
    -------
    current_level_result: (soft/delta) functional dependencies discovered by algorithm, data structure: a list of candidates where candidates are in the format of sets
    computational_graph: A dict where key:level value: list of current level's candidates, candidates are in the format of set

    Output
    -------
    A pruned computational graph
    """
    # Candidates are pruned because minimal FD are already discovered

    # prune candidates after this level by verifying whether the next level has previous level's candidates as subset
    new_computational_graph = copy.deepcopy(computational_graph)
    while level<2:
        level+=1
        for LHS in current_level_result:
            for candidate in computational_graph[level]:
                if LHS.issubset(candidate):
                    if candidate in new_computational_graph[level]:
                        new_computational_graph[level].remove(candidate)


    return new_computational_graph
def transform_res(FDs):
    """
    Parameters
    --------------
    FDs: a list of (soft/delta) functional dependencies, where elements are tuples(LHS,RHS), LHS is in the format of set

    Output
    ---------
    current_level_result: a dictionary where key: RHS value: a list of LHS where candidates are in the form of sets
    """

    current_level_result=dict()
    for (LHS,RHS) in FDs:
        if RHS not in current_level_result.keys():
            current_level_result[RHS]=[]

        current_level_result[RHS].append(LHS)

    return current_level_result


# In[14]:


def find_softFDs_pairs(level, df,current_level_candidates):
    softFDs=[]
    rdds=spark.sparkContext.emptyRDD()
    for RHS in current_level_candidates.keys():
        for LHS in current_level_candidates[RHS]:
            rddt=df.rdd.map(lambda x: (*LHS, RHS, tuple(x[idx] for idx in list(map(lambda y: schema.index(y),LHS))), x[schema.index(RHS)]))
            rdds=rdds.union(rddt)
    
    rdds = rdds.map(lambda tpe: (tpe,1)).reduceByKey(add)                .map(lambda x: ((x[0][:-1]), ((x[0][-1], x[1]), x[1])))                .repartition(1)                .aggregateByKey(zeroVal1,mergeVal1,mergeComb1)                .map(lambda x: ((x[0]), list(map(lambda r: round(r[1] / x[1][1], 3),x[1][0]))))                .map(lambda x: ((x[0]), 1 - reduce(lambda p1, p2:p1+p2,list(map(lambda p: p*(1-p), x[1])))))                .map(lambda x: ((x[0][:-1]), (x[0][-1], x[1]))).aggregateByKey(zeroVal3,mergeVal3,mergeComb3)                .filter(lambda tup: all(t >= 0.8 for t in tup[1][1]))                .map(lambda x:(*x[0][:level],x[0][level]))
                
    for item in rdds.toLocalIterator():
        softFDs.append(({*item[:-1]},item[-1]))

    return softFDs


# In[15]:


computational_graph=dict()
for RHS in schema:
    computational_graph[RHS]=generate_computational_graph(RHS,schema)

#Define current_level_candidates
current_level_candidates=dict()

for RHS in schema:
    current_level_candidates[RHS] = get_candidates(0,computational_graph[RHS])


# In[16]:


current_level_candidates


# In[17]:


start_time = time.time()
softFDs = find_softFDs_pairs(1, toy_df, current_level_candidates)
print("--- %s seconds ---" % (time.time() - start_time))
print(softFDs)


# In[ ]:




