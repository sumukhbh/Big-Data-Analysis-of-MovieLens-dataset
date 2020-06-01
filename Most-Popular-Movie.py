# -*- coding: utf-8 -*-
"""
Created on Sun May 31 16:19:03 2020

@author: sumukh
"""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MostPopularMovie")
sc = SparkContext(conf=conf)

def movienames():
    movie_name_mapping = {}
    file_open = open("ml-100k/u.item")
    for item in file_open:
        item = item.split("|")
        # creating a movie id to name mapping #
        movie_name_mapping[int(item[0])] = item[1]
    return movie_name_mapping

# using a broadcast variable for transmitting information to every node in the cluster #
movie_names_dict = sc.broadcast(movienames())
            
    

input_rdd = sc.textFile("file:///SparkCourse/ml-100k/u.data")
movie_id_rdd = input_rdd.map(lambda x: (int(x.split()[1]),1))
count_rdd = movie_id_rdd.reduceByKey(lambda x,y:x+y)

sort_rdd = count_rdd.map(lambda x:(x[1],x[0])).sortByKey(ascending=False)
name_added_rdd = sort_rdd.map(lambda x: (movie_names_dict.value[x[1]], x[0]))
results = name_added_rdd.collect()

for value in results:
    print(value[0], ":", value[1])
    

