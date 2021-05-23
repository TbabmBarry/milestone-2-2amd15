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


# In[3]:


df = spark.read.format("csv").option("header", "true").load('./data/2018-01_bme280sof.csv')


# In[4]:


df = df.drop('_c0')


# In[5]:


df.createOrReplaceTempView("sofia")


# In[6]:


small_df = spark.sql("select * from sofia limit 1000")


# In[7]:


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
        if level == 0:
            for attribute in schema:
                if attribute != RHS:
                    computational_graph[level].append(set([attribute]))

        else:
            for element1 in computational_graph[level-1]:
                for element2 in computational_graph[0]:
                    newelement = element1.union(element2)
                    if newelement not in computational_graph[level]:
                        if len(newelement) == level + 1:
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
  
    for LHS in current_level_result:
        for candidate in computational_graph[level+1]:
            if LHS.issubset(candidate):
                if level<2:
                    new_computational_graph[level+1].remove(candidate)


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
        current_level_result[RHS]=LHS

    return current_level_result


# In[8]:


def controller(df, func):
    """
    A control flow function

    Parameters
    -----------
    func: (soft/delta) Functional Discovery functions
    df: dataframe
    """  
  # Initialization: Generate computational graph for each attribute which will be on RHS
    schema = df.columns[1:]
    computational_graph=dict()
    for RHS in schema:
        computational_graph[RHS]=generate_computational_graph(RHS,schema)

    for level in range(3):
        # Get current level candidates
        current_level_candidates=dict()
        for RHS in schema:
            current_level_candidates[RHS] = get_candidates(level,computational_graph[RHS])
    
        # Use current_level candidates as an input to FD-functions for each level, func will return discovered (soft/delta)functional dependencies
        FDs = func(df,current_level_candidates)
    
        #Transform res into a dictionary where key: RHS value: a list of LHS where candidates are in the form of sets
        current_level_result = transform_res(FDs)
    
        # Prune graphs according to feedback of FD-functions
        for RHS in schema:
            computational_graph[RHS] = prune_graph(level, current_level_result[RHS],computational_graph[RHS]) 


# In[9]:


schema = small_df.columns


# In[27]:


computational_graph=dict()
for RHS in schema:
    computational_graph[RHS]=generate_computational_graph(RHS,schema)

# Transform res into a dictionary where key: RHS value: a list of LHS
current_level_result = dict()
for RHS in schema:
    current_level_result[RHS] = [{'location', 'temperature'}]

# Prune graphs according to feedback of FD-functions
for RHS in schema:
    computational_graph[RHS]=prune_graph(1, current_level_result[RHS],computational_graph[RHS]) 

current_level_candidates=dict()
for RHS in schema:
    current_level_candidates[RHS] = get_candidates(1,computational_graph[RHS])


# In[10]:


# combine into a four-tuple rdd
def createCombiner(a):
    return [a]

def append(a, b):
    a.append(b)
    return a

def extend(a, b):
    a.extend(b)
    return a


# In[11]:


LHS = {'location', 'lon'}
RHS = 'humidity'


# In[13]:


# map and reduce into a five-tuple rdd
rdd1 = small_df.rdd.map(lambda x: (tuple(LHS), RHS, tuple(x[idx] for idx in list(map(lambda y: schema.index(y),LHS))), x[schema.index(RHS)])).map(lambda tpe: (tpe,1)).reduceByKey(lambda a,b:a+b)
# remap five-tuple key and combine by X,A key
# Method 1: groupByKey + mapValues
# rdd2 = rdd1.map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[0][3], x[1]))).groupByKey().mapValues(lambda xs: (tuple(xs)))
# Method 2: combineByKey
rdd2 = rdd1.map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[0][3], x[1]))).combineByKey(createCombiner, append, extend).collect()
for line in rdd2:
    print(line)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




