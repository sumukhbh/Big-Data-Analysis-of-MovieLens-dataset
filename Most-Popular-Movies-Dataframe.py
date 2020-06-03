from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("PopularMovies").getOrCreate()

# function which creates a mapping between movie id and movie name #
def name_mapping():
    movie_names = {}
    file_open = open("ml-100k/u.item", "r")
    for line in file_open:
        line = line.split("|")
        movie_names[int(line[0])] = line[1]
    return movie_names

name_id_dict = name_mapping()
input_rdd =  spark.sparkContext.textFile("file:///SparkCourse/ml-100k/u.data")

# let's create a row object from the rdd containing only the movie id #
movie = input_rdd.map(lambda x: Row(movieID = int(x.split()[1])))

# converting to a dataframe #
movie_dataframe = spark.createDataFrame(movie)

# SQL based count #
count_results = movie_dataframe.groupBy("movieID").count().orderBy("count", ascending=False).cache()

# display the results  #
count_results.show()

# filter out the top 10 movies #
top10_movies = count_results.take(10)

# Print the ID and name of the top 10 movies #
for movie in top10_movies:
    print(name_id_dict[movie[0]],movie[1])

spark.stop()