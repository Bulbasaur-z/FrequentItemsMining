import itertools
from collections import defaultdict
from pyspark import SparkContext, SparkConf
import os
import sys
import time

os.environ['PYSPARK_PYTHON'] = '/Users/avalon/anaconda3/envs/DataMining/bin/python3.6'
os.environ['PYSPARK_DRIVE_PYTHON'] = '/Users/avalon/anaconda3/envs/DataMining/bin/python3.6'


def mkString(vList, size):
    if size == 1:
        return ",".join(["('{}')".format(x) for x in vList]) + "\n"
    else:
        return ",".join([str(x) for x in vList]) + "\n"


def seqFunc(x, y):
    x.add(y)
    return x


def seqFunc2(x, y):
    x.append(y)
    return x

# when setSize greater than 2
def candidateConstruct(chunkThreshold, setSize, chunk, filterSets):
    candidate = dict()
    length = len(filterSets)
    for i in range(length):
        subset1 = filterSets[i][:setSize - 2]
        for j in range(i + 1, length):
            subset2 = filterSets[j][:setSize - 2]
            if subset1 == subset2:
                l = set(filterSets[i])
                r = set(filterSets[j])
                candidate[tuple(sorted(l.union(r)))] = 0
    for basket in chunk:
        for p in candidate.keys():
            if set(p).issubset(basket):
                candidate[p] += 1
    return [x for x in candidate.keys() if candidate[x] >= chunkThreshold]


def SONFirstMap(chunkThreshold):
    def PCYHash(m, n):
        return (hash(m) + hash(n)) % int(1e8)

    def executor(chunk):
        storedChunk = list()
        nameCountMap = defaultdict(int)
        PCYFilter = defaultdict(int)
        results = list()
        # count frequent singletons
        for basket in chunk:
            storedChunk.append(basket)
            for single in basket:
                nameCountMap[single] += 1
            for pair in itertools.combinations(basket, 2):
                key = PCYHash(pair[0], pair[1])
                PCYFilter[key] += 1
        # PCY filter
        # frequent singleton
        results.append([x for x in nameCountMap.keys() if nameCountMap[x] >= chunkThreshold])
        frequentPairCandidates = dict()

        for p in itertools.combinations(results[0], 2):
            key = PCYHash(p[0], p[1])
            if key in PCYFilter and PCYFilter[key] >= chunkThreshold:
                frequentPairCandidates[tuple(sorted(p))] = 0
        del PCYFilter
        for basket in storedChunk:
            for p in frequentPairCandidates.keys():
                if set(p).issubset(basket):
                    frequentPairCandidates[p] += 1
        results.append([x for x in frequentPairCandidates.keys() if frequentPairCandidates[x] >= chunkThreshold])

        # count others
        for c in itertools.count(start=3, step=1):
            if len(results[c - 2]) < c:
                break
            results.append(candidateConstruct(chunkThreshold=chunkThreshold,
                                                 setSize=c,
                                                 chunk=storedChunk,
                                                 filterSets=results[c - 2]))
        for i in range(len(results)):
            yield i + 1, set(results[i])

    return executor


def SONSecondMap(allCandidates):
    def countExecutor(chunk):
        frequentItemSets = defaultdict(int)
        for basket in chunk:
            for oneSizeSets in allCandidates:
                for candidate in oneSizeSets[1]:
                    if isinstance(candidate, tuple):
                        if set(candidate).issubset(basket):
                            frequentItemSets[candidate] += 1
                    else:
                        if candidate in basket:
                            frequentItemSets[candidate] += 1
        for k, v in frequentItemSets.items():
            if isinstance(k, tuple):
                yield (len(k), k), v
            else:
                yield (1, k), v

    return countExecutor


if __name__ == "__main__":
    conf = SparkConf()
    conf.setAppName("Task1").setMaster("local[*]") \
        .set("spark.executor.memory", "4g") \
        .set("spark.driver.memory", "4g")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    start = time.time()
    rawData = sc.textFile(sys.argv[3]).filter(lambda _: _ != 'user_id,business_id')
    support = int(sys.argv[2])
    filterSupport = int(sys.argv[1])
    KVPairs = rawData.map(lambda s: tuple(s.split(",")))
    basketModule = KVPairs.aggregateByKey(zeroValue=set(),
                                          seqFunc=seqFunc,
                                          combFunc=lambda x, y: x.union(y),
                                          numPartitions=int(support / 10)) \
        .filter(lambda x: len(x[1]) > filterSupport) \
        .values().persist()

    candidates = basketModule.mapPartitions(SONFirstMap(10)).reduceByKey(
        lambda x, y: x.union(y)).collect()

    FrequentItemSets = basketModule.mapPartitions(SONSecondMap(candidates)) \
        .reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] >= support)\
        .keys()\
        .aggregateByKey(zeroValue=list(),
                        seqFunc=seqFunc2,
                        combFunc=lambda x, y: x + y).collect()

    #write into file
    forOutput = dict()
    largestSize = 0
    for v in candidates:
        forOutput[v[0]] = sorted(v[1])
        if v[0] > largestSize:
            largestSize = v[0]
    with open(sys.argv[4], mode='w', encoding='utf-8') as f:
        f.write("Candidates:\n")
        for n in range(1, largestSize+1):
            f.write(mkString(forOutput[n], n))
        largestSize = 0
        forOutput.clear()
        for v in FrequentItemSets:
            forOutput[v[0]] = sorted(v[1])
            if v[0] > largestSize:
                largestSize = v[0]
        f.write("\nFrequent Itemsets:\n")
        for n in range(1, largestSize+1):
                f.write(mkString(forOutput[n], n))

    print("Duration: {}".format(str(time.time() - start)))
