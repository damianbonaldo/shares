set PYTHON_HOME=C:\Users\aed5041\AppData\Local\Programs\Python\Python39
set PYSPARK_PYTHON=C:\Users\aed5041\AppData\Local\Programs\Python\Python39\python.exe
set SPARK_HOME=D:\spark-3.2
set JAVA_HOME=C:\Program Files\Java\jdk1.8.0_202
set JAVA_BIN=%JAVA_HOME%\bin
set HADOOP_HOME=C:\tmp\hive
set FILE_NAME=%1

spark-submit --master spark://127.0.0.1:7077 %FILE_NAME% --jars D:\spark-3.2\jars\ojdbc6.jar --verbose
