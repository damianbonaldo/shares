from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    fields=line.split(';')
    age=int(fields[2])
    numFriends=int(fields[3])
    return (age, numFriends)

config = SparkConf().setMaster("local").setAppName("AmigosPorEdad")

sc = SparkContext(conf = config)

lines = sc.textFile("D://mis cosas//Development//python//20221213 - spark//amigos.csv")
rdd = lines.map(parseLine)

totalsByAge=rdd.mapValues(lambda x: (x, 1) ).reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results=averagesByAge.collect()
for r in results:
    print(r)