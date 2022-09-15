import findspark
import pyspark
findspark.init('C:\spark\spark-3.3.0-bin-hadoop3')
import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains,udf
from pyspark.sql. functions import lit
from pyspark.sql. functions import *
from pyspark.sql import functions as F
from IPython.display import display
from flatten_json import unflatten_list
from flatten_json import flatten
import pandas as pd
import pprint


spark = SparkSession.builder.master("local[2]").config("spark.jars", "E:\Azure Data Engineer\MYSQL_CONNECTOR_8_0_23\mysql-connector-java-8.0.23.jar").config("spark.ui.port","4050").appName("Reading the csv file and reading Mysql Tables ").getOrCreate()
print(spark)
#spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setLogLevel("OFF")


print("the spark is running in -->> ")
print(spark.sparkContext.uiWebUrl)
print(spark.sparkContext.version)
spark.conf.set("spark.sql.repl.eagerEval.enabled",True)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

forward_fact_dataframe = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .csv("cp_bi_prod_sales__forward_unit_fact - cp_bi_prod_sales__forward_unit_fact.csv").cache()
#display(forward_fact_dataframe.show(5,truncate=False))
print(forward_fact_dataframe.columns)



forward_fact_dataframe = forward_fact_dataframe.withColumn("CURRENT_TIME",F.current_timestamp())
print(len(sorted(forward_fact_dataframe.columns)))
forward_fact_dataframe.printSchema()

selected_columns = forward_fact_dataframe.select("account_id","seller_id","gmv","status","order_external_id","CURRENT_TIME")
selected_columns.show(truncate=False)

##connecting to sql database
mysql_driver_class = "com.mysql.jdbc.Driver"
table_name="information_schema.tables"
host_name="localhost"
port_no=str(3306)
user_name = "root"
password="shrinjit"
database_name ="sakila"

mysql_jdbc_url="jdbc:mysql://"+host_name+":"+port_no+"/"+database_name
print(mysql_jdbc_url)
import warnings
warnings.filterwarnings('ignore')
mysql_table_df = spark.read.format('jdbc')\
                .option("url",mysql_jdbc_url)\
                .option("driver",mysql_driver_class)\
                .option("dbtable",table_name)\
                .option("user",user_name)\
                .option("password",password).load()

json_data = {
    "a": 1,
    "b": 2,
    "c": [{"d": [2, 3, 4], "e": [{"f": 1, "g": 2}]}]
}
flatten_data=flatten(json_data)
print(flatten_data)

acschema = StructType([
    StructField("a", IntegerType(), True),
    StructField("b", IntegerType(), True),
    StructField("c_0_d_0", IntegerType(), True),
    StructField("c_0_d_1", IntegerType(), True),
    StructField("c_0_d_2", IntegerType(), True),
    StructField("c_0_e_0_f", IntegerType(), True),
    StructField("c_0_e_0_g", IntegerType(), True)


])


df = pd.DataFrame(flatten_data,index=[0])
final_df = spark.createDataFrame(df,schema=acschema)
#final_df.show()
#print(final_df.printSchema())



## this will show the output 
print("####################")
store_data = {
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}


flatten_store_data = flatten(store_data)
#print(flatten_store_data)

sensor_df = pd.DataFrame(flatten_store_data,index=[0])
#sensor_df.head()
sensor_data_final = spark.createDataFrame(sensor_df)
sensor_data_final.show()
print(sensor_data_final.columns)

print('the  schema of the final data frame created -->>> ')
sensor_data_final.printSchema()