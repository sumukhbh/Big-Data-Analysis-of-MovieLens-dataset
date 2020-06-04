# Item based Collaborative Filtering #
import sys, math
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local[*]").setAppName("SimilarMovies")
sc = SparkContext(conf=conf)

# function to create a mapping between movie ids and names #
def name_id_mapping():
    movie_names = {}
    file_open = open("ml-100k/u.item", "r")
    for line in file_open:
        line = line.split("|")
        movie_names[int(line[0])] = line[1]
    return movie_names

def filtervalues(userrating):
      ratings = userrating[1]
      (movieid1,rating1) = ratings[0] # first movie id and it's rating #
      (movieid2,rating2) = ratings[1] # second movie id and it's rating #
      return movieid1 < movieid2

def mapmovies(userrating):
    ratings = userrating[1]
    (movieid1,rating1) = ratings[0]
    (movieid2,rating2) = ratings[1]
    return ((movieid1,movieid2),(rating1,rating2))

def cosine_similarity(ratings):
    sum_aa = 0
    sum_bb = 0
    sum_ab = 0
    no_pairs = 0
    for ratingA,ratingB in ratings:
        sum_aa  += ratingA * ratingA
        sum_bb  += ratingB * ratingB
        sum_ab  += ratingA * ratingB
        no_pairs += 1
    numerator = sum_ab
    denominator = math.sqrt(sum_aa) * math.sqrt(sum_bb)
    similarity_score = 0
    if denominator != 0:
        similarity_score = (numerator/(float(denominator)))
    return (similarity_score,no_pairs)

name_dict = name_id_mapping()

# read the input file into a RDD #
input_rdd = sc.textFile("file:///SparkCourse/ml-100k/u.data")

# Let's map the input to the form (userid,(movieid,rating)) #
rating_values = input_rdd.map(lambda x: x.split()).map(lambda x : (int(x[0]),(int(x[1]),float(x[2]))))

# Let's do a join operation in order to generate all the movies watched and rated by the same user #
joined_pairs = rating_values.join(rating_values)
#print(joined_pairs.take(5))

# Now we will have to filter out the duplicate values. Let's do it using filter #
filtered_pairs = joined_pairs.filter(filtervalues)
#print(filtered_pairs.take(5))

# Now we will just map our input to the form ((movieid1,rating1),(movieid2,rating2)) because the userID is no longer required #
movie_pairs = filtered_pairs.map(mapmovies)
#print(movie_pairs.take(5))

# now let's group by key #
movies_group = movie_pairs.groupByKey()

# Let's compute the similarity using cosine similarity measure #
movie_similarities = movies_group.mapValues(cosine_similarity).cache()
#print(movie_similarities.take(5))

# Now let's extract the movies which are similar to the one we care about #

if len(sys.argv[1]) > 1:
    coOccurenceThreshold = 50
    scoreThreshold = 0.97
    movie_id = int(sys.argv[1])
    # let's use these above metrics to filter out the movies #
    similar_movies = movie_similarities.filter(lambda moviePair: (moviePair[0][0] == movie_id or moviePair[0][1] == movie_id) and moviePair[1][0] > scoreThreshold and moviePair[1][1] > coOccurenceThreshold)
    #print(similar_movies.take(5))

    # Now we will sort the results and consider only the top 10 results #
    sorted_pairs = similar_movies.map(lambda x: (x[1],x[0])).sortByKey(ascending=False).take(10)
    print(sorted_pairs)

    # Finally we will map the movie ids to their respective names #
    print("The Top 10 similar movies for ",name_dict[movie_id])
    for ratings in sorted_pairs:
            (sim, pair) = ratings
            similarMovieID = pair[0]
            if (similarMovieID == movie_id):
                similarMovieID = pair[1]
            print(name_dict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))





