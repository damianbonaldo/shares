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
#df = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:oracle:thin:@172.21.5.140:1551:PREESERCIZIO", "user" -> "ESEPSW", "password" -> "ESEPSW", "select * from auabrppa_tab" -> Query, "driver" -> "oracle.jdbc.driver.OracleDriver")).load()
#df = sqlContext.read.format('jdbc').options(url=Oracle_CONNECTION_URL,dbtable="select * from auabrppa_tab",  driver="oracle.jdbc.OracleDriver").load() 
#       .option("url", "jdbc:oracle:thin:@your_aliastns?TNS_ADMIN=path/to/wallet") \
#172.21.5.140:1551:PREESERCIZIO
df = sqlContext.read.format("jdbc").option("url", "jdbc:oracle:thin:@172.21.5.140:1551:prod01a").option("dbtable", '(select id_rppa from auabrppa_tab where rownum < 10)').option("user", "ESEPSW").option("password", "ESEPSW").option("driver", "oracle.jdbc.driver.OracleDriver").load()
df.show()

