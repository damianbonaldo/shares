from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    fields=line.split(';')
    station=fields[0]
    tempType=fields[1]
    entry=int(fields[2])
    temperature=float(fields[3])*0.1*(9.0/5.0)+32.0
    return (station, tempType, entry, temperature)

config = SparkConf().setMaster("local").setAppName("AmigosPorEdad")

sc = SparkContext(conf = config)

lines = sc.textFile("D://mis cosas//Development//python//20221213 - spark//1800.csv")
rdd = lines.map(parseLine)


minTemps=rdd.filter(lambda x: "TMIN" in x[1])
stationTemps=minTemps.map(lambda x: (x[0],x[3]))
Temp_min=stationTemps.reduceByKey(lambda x,y : min(x,y))

results=Temp_min.collect()
for r in results:
    print(r)