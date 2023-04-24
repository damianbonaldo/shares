from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

ORACLE_DRIVER_PATH = "D:\spark-3.2\jars\ojdbc6.jar"                                            
#Oracle_CONNECTION_URL ="jdbc:oracle:thin:ESEPSW/ESEPSW@172.21.5.140:1551/PREESERCIZIO"    
#Oracle_CONNECTION_URL ="jdbc:oracle:thin:ESEPSW/ESEPSW@172.21.5.140:1551:PREESERCIZIO"    
Oracle_CONNECTION_URL ="jdbc:oracle:thin:ESEPSW/ESEPSW@PREESERCIZIO"    

config = SparkConf()
config.setMaster("local")
config.setAppName("Oracle_imp_exp") 
sc = SparkContext(conf = config)      
sqlContext = SQLContext(sc)
df = sqlContext.read.format("jdbc").option("url", "jdbc:oracle:thin:@172.21.5.140:1551:prod01a").option("dbtable", "(select view_name, text from all_views where owner = 'ESEPDBAE')").option("user", "ESEPSW").option("password", "ESEPSW").option("driver", "oracle.jdbc.driver.OracleDriver").load()

#df.select("view_name").filter("upper(text) like '%CAPI%'").show()
#rdd2=df.rdd.map(lambda x: (x["view_name"],x["text"]))
#rdd2=df.rdd.map(lambda x: (x.view_name,x.text))
rdd2=df.rdd.map(lambda x: (x[0],x[1]))

for r in rdd2.collect():
    print(r)

#df = spark.read \
#    .format("jdbc") \
#    .option("url", "JDBC_URL") \
#    .option('driver', 'oracle.jdbc.driver.OracleDriver') \
#    .option("oracle.jdbc.timezoneAsRegion", "false") \
#    .option("sessionInitStatement", plsql_block) \
#    .option("dbtable", count_query) \
#    .option("user", "USER_ID") \
#    .option("password", "PASSWORD") \
#    .load()

# df = sqlContext.createDataFrame([(10, 'ZZZ')],["id", "name"])
# df.write.insertInto('testtdb.sample_tab1',overwrite = False)
