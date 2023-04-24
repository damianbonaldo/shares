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
df.shows()  
df.select("view_name").filter("upper(text) like '%CAPI%'").show()
