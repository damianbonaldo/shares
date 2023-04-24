from pyspark import SparkConf, SparkContext
import collections

config = SparkConf().setMaster("local").setAppName("MoviesRating")

sc = SparkContext(conf = config)

lines = sc.textFile("D://mis cosas//Development//python//20221107 - Spark//ml-100k//u.data")

ratings = lines.map(lambda x : x.split()[2] )

result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))

for key,value in sortedResults.items():
    print("%s %i" % (key,value))
