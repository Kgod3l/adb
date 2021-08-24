# Databricks notebook source
class Curation:
  __slots__ = ['filepath','fileSchema','rejectPath']
 #initializing the class 
  def __init__(self, **kwargs):
    for key, value in kwargs.items():
      setattr(self,key,value)
      
  #function to read a file by inferring schema
  def readFileInferSchema(self):
    readDf = spark.read.option("badRecordsPath", self.rejectPath).csv(self.filepath,header="true",inferSchema="true")
    return readDf
  
  #function to read a file adhering to user defined schema
  def readFileStrictMode(self):
    readDf = spark.read.option("badRecordsPath", self.rejectPath).schema(self.fileSchema).csv(self.filepath,header="true",inferSchema="true")
    return readDf
  
  #function to trim all the columns in dataframe
  def trimCol(self,df):
    for c_name in df.columns:
      df = df.withColumn(c_name, trim(col(c_name)))
      return df  
  
  #function to read external databases using jdbc
  def readDbViaJdbc(self,jdbcparams):
    queryDf = spark.read.format("jdbc").option("url", jdbcUrl).option("query", jdbcQuery).option("user", jdbcuser).option("password", jdbcpwd).option("driver", jdbcDriver).load()
    return queryDf
  
  #function to write dataframes to sink database
  def sink(self,df):
    df.write.csv(path=targetPath,mode="overwrite")
