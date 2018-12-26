import findspark
findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import numpy as np
from functools import reduce
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
from itertools import combinations
import timeit

min_sup=0.1

def maptodict(transactions):
    """"Map each partition of transactions onto one dictionary. The
    keys of dicitonary are all the items contained in the transactions
    and the value of each key is an array whose length is number of
    transactions contained in this partition. The array describes whether
    one item is contained in one transaction"""
    tlist=list(transactions)
    length=len(tlist)
    keys=list(set([item for sublist in tlist for item in sublist]))
    db=dict((key,np.zeros((len(tlist),),dtype=int)) for key in keys) 
    for i in range(len(tlist)):
        for key in tlist[i]:
            db[key][i]=1 
    candidates=[[key] for key in sorted(keys)]
    return db,candidates,length

def get_candidates(itemSet, length):
    """Join a set with itself and returns the n-element itemsets."""
    return [list(x) for x in set(frozenset(i).union(frozenset(j)) for i in itemSet for j in itemSet if len(set(i).union(set(j))) == length)]

def get_candidates2(itemSet):
    """Improved join method by reducing number of iterations"""
    candidates=set()
    for i in range(len(itemSet)):
        for j in range(i+1,len(itemSet)):
            if itemSet[i][0:-1]==itemSet[j][0:-1] and itemSet[i][-1]<itemSet[j][-1]:
                    candidates.add(frozenset(itemSet[i]).union(frozenset(itemSet[j])))
            else:
                break
    return [list(x) for x in candidates]

def pre_prune(itemSet,length):
    """Preprune frequent itemsets which cannot generate higher dimensional frequent itemsets. 
    The condition is that the frequent itemset should not contain any item whose frequency 
    of appearing in the itemsets is lower than the length of the itemset."""
    items=set([item for itemset in itemSet for item in itemset])
    freq=[len([itemset for itemset in itemSet if i in itemset]) for i in items]
    item=[i for i in items]
    item_freq=dict(zip(item,freq))
    filter_item=[item for item,freq in item_freq.items() if freq<length]
    filter_itemSet=[i for i in itemSet if not any(item in i for item in filter_item)]
    return filter_itemSet

def prune(candidates,itemSet):
    """Generate all k-1 length subsets for each k-candidate. If any subset of one
    candidate is not contained in k-1 frequent itemsets, prune the candidate."""
    filter_candidates=candidates.copy()
    itemSet=set(frozenset(item) for item in itemSet)
    for candidate in candidates:
        subsets=[set(i) for i in combinations(candidate,len(candidate)-1)]
        if any(subset not in itemSet for subset in subsets):
            filter_candidates.remove(candidate)
    return filter_candidates
        
def get_fre_itemsets(dic,candidates,transaction_num):
    """Get support for each candidate based on the dictionary and get frequent
    item sets."""
    fre_itemsets=[]
    for candidate in candidates:
         value=[dic[key] for key in candidate]
         fre=reduce(lambda x,y: x&y,value).sum()
         sup=fre/transaction_num
         if sup>min_sup:
             fre_itemsets.append(candidate)
    return fre_itemsets
    

""" Improved Apriori algorithm applied on the dictionary.
Each dictionary is extracted from one partition of transactions.
Get local frequent itemsets for each partition."""
def IAP(transactions):
    """candidate length"""
    candidate_let=1
    """Get candidates of length 1 first."""
    dic,candidates,length=maptodict(transactions)
    """Get frequent itemsets of length 1"""
    fre_itemsets=get_fre_itemsets(dic,candidates,length)
    fre_itemsets_bro=fre_itemsets.copy()
    while True:
        try:
            fre_itemsets_bro=pre_prune(fre_itemsets_bro,candidate_let)
            candidate_let+=1
            candidates=get_candidates2(sorted(fre_itemsets_bro))
            candidates=prune(candidates,fre_itemsets_bro)
            fre_itemsets_bro=get_fre_itemsets(dic,candidates,length)
            if len(fre_itemsets_bro)>0:
                for itemset in fre_itemsets_bro:
                    fre_itemsets.append(itemset)
            else:
                break   
        except Exception as e:
            print(e)
            break
    yield fre_itemsets
    
def SparkApriori(transactions,minSupport):
    global min_sup
    min_sup=minSupport
    
    """For each partition of transactions, get the local frequent itemset.
       Merge the local frequent itemsets as the global candidates."""
    global_candidates=transactions.mapPartitions(IAP).flatMap(lambda its:[frozenset(it) for it in its]).distinct()
    
    """Broadcast transactions to each worker node. Then every worker can scan transactions
       directly from the broadcast value."""
    original_data=sc.broadcast(transactions.collect())
    
    """For each candidate find its frequency by scanning the transactions.
    Filter candidates by calculating their support.Finally collect the rdd
    as map where the key is the frequent itemset and the value is the support
    of the frequent itemset."""
    global_frequent_itemsets=global_candidates.map(lambda x:(x,len([transaction for transaction in original_data.value if x.issubset(frozenset(transaction))])/len(original_data.value)))\
    .filter(lambda x: x[1]>=min_sup).collectAsMap()

    return global_frequent_itemsets

def printresult(itemsets):
    for itemset in itemsets:
        print("frequent itemset(itemset=%s, support=%.2f)" % (str([i for i in itemset]),round(itemsets[itemset],2)))
#        print("support: %s , %.3f" % (str(itemsets[itemset])))


if __name__ == '__main__':
    path='/home/zyt/bdt/5003/T10I4D100K.dat'
    transactions=spark.sparkContext.textFile(path,10).map(lambda x: [y for y in x.split()])
    copytimes=5
    ntransaction=spark.sparkContext.emptyRDD()
    for i in range(0,copytimes+1):
       ntransaction=ntransaction.union(transactions)
    start = timeit.default_timer()
    haha=SparkApriori(transactions,0.002)
    elapsed=timeit.default_timer()-start
    print(elapsed)
    #printresult(haha)
    
    
    
    
        
    

