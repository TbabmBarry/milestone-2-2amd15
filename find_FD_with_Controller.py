#!/usr/bin/env python
# coding: utf-8

# In[2]:


import copy
import findspark
findspark.init()


# ### Import Data

# In[3]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
sc=spark.sparkContext


# In[20]:


df = spark.read.format("csv").option("header", "true").load('./data/2018-01_bme280sof.csv')


# In[4]:


# df = spark.sql("SELECT * FROM _2018_01_bme280sof_1_csv")


# ### Define Controller(a function not a class for convenience)

# In[21]:


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


def find_FDs(df,current_level_candidates):
    """
    Parameters
    -------------
    df: dataframe
    current_level_candidates: A dictionary where key:RHS value: a list of LHS, LHS are in a set format

    Output
    ---------
    A list of discovered functional dependencies
    """
  
    schema = df.columns[1:]
    FDs=[]
    for RHS in schema:
        for LHS in current_level_candidates[RHS]:
            sqlstring='SELECT '+f'{", ".join(f"{attribute}" for attribute in LHS)}'+f', COUNT(DISTINCT {RHS}) c'+' FROM _2018_01_bme280sof_1_csv GROUP BY '+f'{", ".join(f"{attribute}" for attribute in LHS)}'+ ' HAVING c>1'
            res = spark.sql(sqlstring).count()
            if(res==0):
                FDs.append((LHS,RHS))

    return FDs


# In[22]:


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
            computational_graph[RHS]=prune_graph(level, current_level_result[RHS],computational_graph[RHS]) 


# In[28]:


#Function Sanity Check
schema = df.columns[1:]
computational_graph=dict()
for RHS in schema:
    computational_graph[RHS]=generate_computational_graph(RHS,schema)

# print(computational_graph['sensor_id'][2])

#Transform res into a dictionary where key: RHS value: a list of LHS
current_level_result = dict()
for RHS in schema:
    current_level_result[RHS] = [{'location', 'temperature'}]

# Prune graphs according to feedback of FD-functions
for RHS in schema:
    computational_graph[RHS]=prune_graph(1, current_level_result[RHS],computational_graph[RHS]) 

# print(computational_graph['sensor_id'][2])

current_level_candidates=dict()
for RHS in schema:
    current_level_candidates[RHS] = get_candidates(1,computational_graph[RHS])

FDs=find_FDs(df,current_level_candidates)
print(FDs)


# In[ ]:




