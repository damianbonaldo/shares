from pyspark import SparkConf, SparkContext
import re
import collections

def loadMovieNames():
    movieNames={}
    with open("D://mis cosas//Development//python//20221107 - Spark//ml-100k//u.item") as f:
        for line in f:
            fields=line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

config = SparkConf().setMaster("local").setAppName("AmigosPorEdad")

sc = SparkContext(conf = config)
moviesDic = sc.broadcast(loadMovieNames())

lines = sc.textFile("D://mis cosas//Development//python//20221107 - Spark//ml-100k//u.data")
movies = lines.map(lambda x : (int(x.split()[1]), 1) )
movieCounts = movies.reduceByKey(lambda x,y: x + y ) 

print(movieCounts)

flipped = movieCounts.map(lambda xy: (xy[1], xy [0])) 
sortedMovies = flipped.sortByKey()


results=sortedMovies.collect()
for r in results:
    print(r)

sortedMovieNames = sortedMovies.map(lambda countMovie: (moviesDic.value[countMovie[1]], countMovie[0]))
resultsNames=sortedMovieNames.collect()

for r in resultsNames:
    print(r)
