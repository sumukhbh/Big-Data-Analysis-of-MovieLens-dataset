# Counting the number of ratings in the movielens dataset #
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("ratings-counter")
sc = SparkContext(conf=conf)

lines_rdd =  sc.textFile("file:///SparkCourse/ml-100k/u.data")
ratings_rdd = lines_rdd.map(lambda x: x.split()[2])
#print(ratings_rdd.collect())
count_ratings =  ratings_rdd.countByValue()
results = collections.OrderedDict(sorted(count_ratings.items()))
for key,value in results.items():
    print(key,value)