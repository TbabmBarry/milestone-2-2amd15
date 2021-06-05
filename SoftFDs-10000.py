import time
import copy
from operator import add
from functools import reduce
from pyspark.sql import SparkSession

def helper(month):
    df = spark.read.csv(path='hdfs:///group/mcs-2amd15-19/data/2018-'+month+'_bme280sof.csv', sep=',', encoding='UTF-8', comment=None, header=True, inferSchema=True)

    final_data = df.na.drop().withColumn("month", lit(month)).withColumn('lat_tmp', format_number(df.lat, 3)).withColumn('lon_tmp', format_number(df.lon, 3)).drop('_c0','lat','lon').withColumnRenamed('lat_tmp','lat').withColumnRenamed('lon_tmp','lon')

    return final_data

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

def find_softFDs_pairs(level, df,current_level_candidates):
    zeroVal1 = ([], 0)
    mergeVal1 = (lambda aggregated, el: (aggregated[0] + [el[0]], aggregated[1] + el[1]))    
    mergeComb1 = (lambda agg1,agg2:agg1+agg2)
    softFDs=[]
    rdds=spark.sparkContext.emptyRDD()
    for RHS in current_level_candidates.keys():
        for LHS in current_level_candidates[RHS]:
            rddt=df.rdd.map(lambda x: (*LHS, RHS, tuple(x[idx] for idx in list(map(lambda y: schema.index(y),LHS))), x[schema.index(RHS)]))
            rdds=rdds.union(rddt)
    
    rdds = rdds.map(lambda tpe: (tpe,1)).reduceByKey(add)\
                .map(lambda x: ((x[0][:-1]), (x[1], x[1])))\
                .repartition(1)\
                .aggregateByKey(zeroVal1,mergeVal1,mergeComb1)\
                .map(lambda x: ((x[0][:-1]), list(map(lambda r: r / x[1][1],x[1][0]))))\
                .map(lambda tup: (tup[0], any(p >= 0.8 for p in tup[1])))\
                .reduceByKey(lambda x, y: x * y)\
                .filter(lambda tup: tup[1] == 1)\
                .map(lambda x:(*x[0][:level],x[0][level]))\
                .collect()
                
    for item in rdds:
      softFDs.append(({*item[:-1]},item[-1]))

    return softFDs

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
  computational_graph=dict()
  FDs=[]
  for RHS in schema:
    computational_graph[RHS]=generate_computational_graph(RHS,schema)

  for level in range(3):
    # Get current level candidates
    current_level_candidates=dict()
    for RHS in computational_graph.keys():
      current_level_candidates[RHS] = get_candidates(level,computational_graph[RHS])

  #     print('candidates:',current_level_candidates)
    # Use current_level candidates as an input to FD-functions for each level, func will return discovered (soft/delta)functional dependencies
    tFDs = func(level,df,current_level_candidates)
  #     print('FDs:',tFDs)
  #     print(tFDs)
    FDs.extend(tFDs)
    #Transform res into a dictionary where key: RHS value: a list of LHS where candidates are in the form of sets
    current_level_result = transform_res(tFDs)
  #     print(current_level_result)

    # Prune graphs according to feedback of FD-functions
  #     print(f"level:{level}, computatioanl_graph_key:{computational_graph.keys()},current_level_result_key:{current_level_result.keys()}")
    for RHS in computational_graph.keys():
      if RHS in current_level_result.keys():
        computational_graph[RHS]=prune_graph(level, current_level_result[RHS],computational_graph[RHS])


  return FDs
if __name__ == '__main__':
    import time
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    df = spark.read.csv(path='hdfs:///2018-01_bme280sof.csv', sep=',', encoding='UTF-8', comment=None, header=True, inferSchema=True)
    df = spark.createDataFrame(df.head(10000), df.schema)
    computational_graph=dict()
    schema = df.columns
    start_time = time.time()
    softFDs = controller(df, find_softFDs_pairs)
    print("--- %s seconds ---" % (time.time() - start_time))
