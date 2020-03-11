from pyspark import SparkContext, SparkConf
import os
import sys
import json

os.environ['PYSPARK_PYTHON'] = '/Users/avalon/anaconda3/envs/DataMining/bin/python3.6'
os.environ['PYSPARK_DRIVE_PYTHON'] = '/Users/avalon/anaconda3/envs/DataMining/bin/python3.6'

def filterState(e):
    data = json.loads(e)
    return data["business_id"], data["state"]


def extract(e):
    data = json.loads(e)
    return data["business_id"], data["user_id"]


if __name__ == "__main__":
    conf = SparkConf()
    conf.setAppName("Task1").setMaster("local[*]") \
        .set("spark.executor.memory", "4g") \
        .set("spark.driver.memory", "4g")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    businessData = sc.textFile(sys.argv[1]).map(filterState).filter(lambda p: p[1] == "NV")
    reviewData = sc.textFile(sys.argv[2]).map(extract).join(businessData).collect()
    with open(sys.argv[3], mode="w", encoding="utf-8") as f:
        f.write("user_id,business_id\n")
        for p in reviewData:
            f.write("{},{}\n".format(p[1][0], p[0]))

