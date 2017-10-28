#Importing sparkSession and other library
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

#Initiating Sparksession
spark = SparkSession.builder.appName('company').getOrCreate()

#Reading in both source files
entities = spark.read.parquet("/Users/krishnaelangovan/Documents/DataLake/UDL2LRF/entities/")
accounts = spark.read.parquet("/Users/krishnaelangovan/Documents/DataLake/UDL2LRF/accounts/")

#default values incase we dont get ABC integration done
dflt = 'unknown'
insert_ts = F.current_timestamp()

#Transformations for company table to lrf
company_sel = entities.join(accounts, entities.code == accounts.entity_code, 'right_outer'). \
            select(F.md5(F.concat(accounts.code, F.from_unixtime(accounts.modified, format='yyyy-MM-dd HH:mm:ss'))).alias('company_key'), \
            accounts.code.alias('account_id'), \
            insert_ts.alias('insert_ts'), \
            F.when(entities.name != ' ', entities.name).otherwise(accounts.name).alias('company_nm'), \
            accounts.entity_code.alias('entity_id'), \
            F.when(accounts.active == 1, accounts.active).otherwise(0).alias('Is_active_ind'), \
            F.when(accounts.role == ' ',dflt).otherwise(dflt).alias('role_nm'), \
            F.from_unixtime(accounts.modified, format='yyyy-MM-dd HH:mm:ss').alias('soruce_ts'))
           
#Not writing these data instead just displaying them on console
company_fnl = company_sel \
              .withColumn("insert_job_run_id", F.lit(-1)) \
              .withColumn("insert_batch_run_id", F.lit(-1)).show(2)
              

#writing out the dataframe to parquet file
#df.write.parquet(company_fnl,
#                 mode='overwrite')
