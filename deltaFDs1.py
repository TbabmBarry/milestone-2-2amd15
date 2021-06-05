import time
import copy
import pyspark as ps
from pyspark.sql import SparkSession
from operator import add
from functools import reduce
import re
from decimal import Decimal
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
sc=spark.sparkContext

conf = ps.SparkConf().setMaster('local').setAppName("deltaFD")
spark=SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.format("csv").option("header", "true").load('/FileStore/tables/test.csv')
df = df.drop('_c0')
df.createOrReplaceTempView("toy")
toy_df = spark.sql("select int(sensor_id), int(location), double(lat), double(lon), timestamp(timestamp), double(pressure), double(temperature), double(humidity) from toy limit 10000")
schema = toy_df.columns

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


def controller(df, func):
    """
    A control flow function

    Parameters
    -----------
    func: (soft/delta) Functional Discovery functions
    df: dataframe

    Output
    ------
    (soft/delta) Functional Dependencies
    """
    # Initialization: Generate computational graph for each attribute which will be on RHS
    schema = df.columns
    computational_graph = dict()
    FDs = []
    for RHS in schema:
        computational_graph[RHS] = generate_computational_graph(RHS, schema)

    for level in range(3):
        # Get current level candidates
        current_level_candidates = dict()
        for RHS in computational_graph.keys():
            current_level_candidates[RHS] = get_candidates(level, computational_graph[RHS])

        #     print('candidates:',current_level_candidates)
        # Use current_level candidates as an input to FD-functions for each level, func will return discovered (soft/delta)functional dependencies
        tFDs = func(level, df, current_level_candidates)
        #     print('FDs:',tFDs)
        #     print(tFDs)
        FDs.extend(tFDs)
        # Transform res into a dictionary where key: RHS value: a list of LHS where candidates are in the form of sets
        current_level_result = transform_res(tFDs)
        #     print(current_level_result)

        # Prune graphs according to feedback of FD-functions
        #     print(f"level:{level}, computatioanl_graph_key:{computational_graph.keys()},current_level_result_key:{current_level_result.keys()}")
        for RHS in computational_graph.keys():
            if RHS in current_level_result.keys():
                computational_graph[RHS] = prune_graph(level, current_level_result[RHS], computational_graph[RHS])

    return FDs


def seperate_candidates(df, current_level_candidates):
    candidates_time = dict()
    candidates_digit = dict()
    
    for RHS in current_level_candidates.keys():
        if RHS == 'timestamp':
            candidates_time[RHS] = current_level_candidates[RHS]
        else:
            candidates_digit[RHS] = current_level_candidates[RHS]
    
    return candidates_time, candidates_digit


def find_deltaFDs_pairs(level, df,current_level_candidates, delta=0.1):
    K = 2
    deltaFDs = [] 
    candidates_time = seperate_candidates(df, current_level_candidates)[0]
    candidates_digit = seperate_candidates(df, current_level_candidates)[1]
    
    
    array_rdds_time = []
    array_rdds_digit = []
    
    for RHS in candidates_time.keys():
        for LHS in candidates_time[RHS]:
            deltaFDs.append((LHS, RHS))
            pairs = df.rdd.map(lambda x:(((*LHS,RHS),*[x[schema.index(attribute)] for attribute in LHS]),{x[schema.index(RHS)]}))
            array_rdds_time.append(pairs)
        
    if len(array_rdds_time) >= 1:
        rdds_time = sc.union(array_rdds_time)
    else:
        rdds_time = spark.sparkContext.emptyRDD()
             
    rdds_time = rdds_time.coalesce(5).reduceByKey(lambda x,y: x.union(y))\
                .map(lambda x: (x[0][0], (max(x[1]) - min(x[1])).total_seconds() / 60 < delta))\
                .filter(lambda x: x[1] == False)\
                .map(lambda x: x[0])\
                .distinct()\
                .collect()
    
    for RHS in candidates_digit.keys():
        for LHS in candidates_digit[RHS]:
            deltaFDs.append((LHS, RHS))
            pairs = df.rdd.map(lambda x:(((*LHS,RHS),*[x[schema.index(attribute)] for attribute in LHS]),{x[schema.index(RHS)]}))
            array_rdds_digit.append(pairs)
        
    if len(array_rdds_digit) >= 1:
        rdds_digit = sc.union(array_rdds_digit)
    else:
        rdds_digit = spark.sparkContext.emptyRDD()
 
    rdds_digit = rdds_digit.coalesce(10).reduceByKey(lambda x,y: x.union(y))\
                .map(lambda x: (x[0][0], (max(x[1]) - min(x[1])) < delta))\
                .filter(lambda x: x[1] == False)\
                .map(lambda x: x[0])\
                .distinct()\
                .collect()
  
    if len(rdds_time) != 0:
        for item in rdds_time:
            deltaFDs.remove(({*item[:-1]},item[-1]))
  
    if len(rdds_digit) != 0:
        for item in rdds_digit:
            deltaFDs.remove(({*item[:-1]},item[-1]))
    
    return deltaFDs


start_time = time.time()
FDs = controller(toy_df,find_deltaFDs_pairs)
print("--- %s seconds ---" % (time.time() - start_time))
print(FDs)
print(len(FDs))
