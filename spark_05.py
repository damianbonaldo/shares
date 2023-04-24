from pyspark import SparkConf, SparkContext
import re
import collections

def normalizeWords(text):
	return re.compile('W+', re.UNICODE).split(text.lower())

config = SparkConf().setMaster("local").setAppName("AmigosPorEdad")

sc = SparkContext(conf = config)

lines = sc.textFile("D://mis cosas//Development//python//20221213 - spark//libro.txt")
#words = lines.flatMap(lambda x: x.split())
words = lines.flatMap(normalizeWords)
wordCount=words.countByValue()


for w, c in wordCount.items():
	cleanWord=word.encode('ascii', 'ignore')
	if(cleanWord):
		print(cleanWord[.decode() + " " + str(c))
        
wordsCount2=words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x + y)
wordsSorted=wordsCount2.map(lambda x: (x[1] , x[0])).sortByKey()
results=wordsSorted.collect()
for r in results:
    print(r)