# Este ejemplo es para trabajar en la nube (AWS) con Spark. 
# la fuente de datos contiene 1.000.000 de registos. 
import sys
from pyspark import SparkConf, SparkContext
from math import sqrt
import collections

def loadMovieNames():
    movieNames={}
    with open("D://mis cosas//Development//python//20221107 - Spark//ml-100k//u.item", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields=line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def makePairs(userRatings):
    ratings = userRatings[1]
    (movie1,rating1)=ratings[0]
    (movie2,rating2)=ratings[1]
    return ((movie1,rating1), (movie2,rating2))
    
def filterDuplicates(userRatings):
    ratings = userRatings[1]
    (movie1,rating1)=ratings[0]
    (movie2,rating2)=ratings[1]
    return movie1 < movie2

def computeCosineSimilarity(ratingPairs):
    numPairs=0
    sum_xx=sum_yy=sum_xy=0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX 
        sum_yy += ratingY * ratingY 
        sum_xy += ratingX * ratingY 
        
    numerator=sum_xy
    denominator=sqrt(sum_xx)*sqrt(sum_xy)
    
    score = 0 
    
    if(denominator):
        score=(numerator/float(denominator))
        
    return (score, numPairs)

config = SparkConf().setMaster("local[*]").setAppName("MoviesSimilaritiesAWS")

#local[*] usa todos los CPU para maximizar la capacidad de computo, 
#         especialmente cuando se usa groupByKey y gran candidad de datos 1.000.000 registros

# Reuso de un RDD --> se debe cachear el RDD para un reuso
#                     la recreacion cuesta mas tiempo, recursos (y esto en casos de netflix o amazon, se puede perder dinero)
#                     SPARK por defecto borra los RDD por ello lo debemos cachear

sc = SparkContext(conf = config)
moviesDic = sc.broadcast(loadMovieNames())

lines = sc.textFile("s3n://datadosis-spark/ml-1m/ratings.dat")
# s3n es el servicio de AWS. 
# para ejecutar el spark-submit se debe hacer 
# spark-submit --executor-memory 1g <nombre_archivo.py>

# en esta linea se mapea el userID, movieID y rating (int(l[0]), (int(l[1]), float(l[2])))
ratings = lines.map(lambda l : l.split().map(lambda l:(int(l[0]), (int(l[1]), float(l[2])))))
partitionedRatings= ratings.partitionBy(100)
joinedRatings = partitionedRatings.join(ratings) 

print(joinedRatings)

uniqueJoinedRatings=joinedRatings.filter(filterDuplicates)

moviePairs=uniqueJoinedRatings.map(makePairs).partitionBy(100)
moviePairRatings=moviePairs.groupByKey()

#Proceso de cache 
#moviePairsSimilarity=moviePairRatings.mapValues(computeCosineSimilarity).cache()
moviePairsSimilarity=moviePairRatings.mapValues(computeCosineSimilarity).persist()
if(len(sys.argv)>1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50
    movieID=int(sys.argv[1])
    
    filteredResults=moviePairsSimilarity.filter(lambda pairSim: (pairSim[0][0] == movieID or pairSim[0][1] == movieID ) and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurrenceThreshold)
    
    results= filterResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending=False).take(10)