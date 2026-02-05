# Databricks notebook source
from pyspark.sql.functions import *
import os
import sys
proj_path = os.path.join(os.getcwd(),'..','..')
sys.path.append(proj_path)

# COMMAND ----------

df_user=spark.readStream.\
    format("cloudFiles").\
    option("cloudFiles.format","parquet").\
    option("cloudFiles.schemaLocation","abfss://silver@datasourceazure.dfs.core.windows.net/DimUser/checkpoint").load("abfss://bronze@datasourceazure.dfs.core.windows.net/DimUser")
    # option("cloudFiles.schemaEvolutionMode","rescue").\
    


# COMMAND ----------

# DBTITLE 1,Cell 3
# display(df_user, checkpointLocation="abfss://silver@datasourceazure.dfs.core.windows.net/DimUser/display_checkpoint")

# COMMAND ----------

df_user=df_user.withColumn("user_name",upper(col("user_name")))

# COMMAND ----------

from utils.transformations import Utilities

# COMMAND ----------

df_user_obj=Utilities()

df_user1=df_user_obj.dropCols(df_user,["_rescued_data"])
df_user1=df_user1.dropDuplicates(["user_id"])

# COMMAND ----------

df_user1.writeStream.outputMode("append").\
    option("checkpointLocation","abfss://silver@datasourceazure.dfs.core.windows.net/DimUser/checkpoint").\
    trigger(once=True).\
    option("path","abfss://silver@datasourceazure.dfs.core.windows.net/DimUser/data").\
    toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC #DimArtist

# COMMAND ----------

df_artist=spark.readStream.\
    format("cloudFiles").\
    option("cloudFiles.format","parquet").\
    option("cloudFiles.schemaLocation","abfss://silver@datasourceazure.dfs.core.windows.net/DimArtist/checkpoint").load("abfss://bronze@datasourceazure.dfs.core.windows.net/DimArtist")
    # option("cloudFiles.schemaEvolutionMode","rescue").\
    


# COMMAND ----------

df_art_obj=Utilities()

df_artist1=df_art_obj.dropCols(df_artist,["_rescued_data"])
df_artist1=df_artist1.dropDuplicates(["artist_id"])

# COMMAND ----------

df_artist1.writeStream.outputMode("append").\
    option("checkpointLocation","abfss://silver@datasourceazure.dfs.core.windows.net/DimArtist/checkpoint").\
    trigger(once=True).\
    option("path","abfss://silver@datasourceazure.dfs.core.windows.net/DimArtist/data").\
    toTable("spotify_cata.silver.DimArtist")


# COMMAND ----------

# MAGIC %md
# MAGIC #DimTrack

# COMMAND ----------

df_track=spark.readStream.\
    format("cloudFiles").\
    option("cloudFiles.format","parquet").\
    option("cloudFiles.schemaLocation","abfss://silver@datasourceazure.dfs.core.windows.net/DimTrack/checkpoint").load("abfss://bronze@datasourceazure.dfs.core.windows.net/DimTrack")
    # option("cloudFiles.schemaEvolutionMode","rescue").\
    


# COMMAND ----------

df_track=df_track.withColumn("duration_flag",when(col("duration_sec")<150,"low").when(col("duration_sec")<300,"medium").otherwise("high"))

df_track=df_track.withColumn("track_name",regexp_replace(col("track_name"),"-"," "))


# COMMAND ----------

df_track_obj=Utilities()

df_track1=df_track_obj.dropCols(df_track,["_rescued_data"])
df_track1=df_track1.dropDuplicates(["track_id"])

# COMMAND ----------

df_track1.writeStream.outputMode("append").\
    option("checkpointLocation","abfss://silver@datasourceazure.dfs.core.windows.net/DimTrack/checkpoint").\
    trigger(once=True).\
    option("path","abfss://silver@datasourceazure.dfs.core.windows.net/DimTrack/data").\
    toTable("spotify_cata.silver.DimTrack")


# COMMAND ----------

# MAGIC %md
# MAGIC #DimDate

# COMMAND ----------

df_date=spark.readStream.\
    format("cloudFiles").\
    option("cloudFiles.format","parquet").\
    option("cloudFiles.schemaLocation","abfss://silver@datasourceazure.dfs.core.windows.net/DimDate/checkpoint").load("abfss://bronze@datasourceazure.dfs.core.windows.net/DimDate")
    # option("cloudFiles.schemaEvolutionMode","rescue").\
    


# COMMAND ----------

df_date_obj=Utilities()

df_date=df_date_obj.dropCols(df_date,["_rescued_data"])

# COMMAND ----------

df_date.writeStream.outputMode("append").\
    option("checkpointLocation","abfss://silver@datasourceazure.dfs.core.windows.net/DimDate/checkpoint").\
    trigger(once=True).\
    option("path","abfss://silver@datasourceazure.dfs.core.windows.net/DimDate/data").\
    toTable("spotify_cata.silver.DimDate")


# COMMAND ----------

# MAGIC %md
# MAGIC #FactStream

# COMMAND ----------

df_stream=spark.readStream.\
    format("cloudFiles").\
    option("cloudFiles.format","parquet").\
    option("cloudFiles.schemaLocation","abfss://silver@datasourceazure.dfs.core.windows.net/FactStream/checkpoint").load("abfss://bronze@datasourceazure.dfs.core.windows.net/FactStream")
    # option("cloudFiles.schemaEvolutionMode","rescue").\
    


# COMMAND ----------

df_stream_obj=Utilities()

df_stream=df_stream_obj.dropCols(df_stream,["_rescued_data"])

# COMMAND ----------

df_stream.writeStream.outputMode("append").\
    option("checkpointLocation","abfss://silver@datasourceazure.dfs.core.windows.net/FactStream/checkpoint").\
    trigger(once=True).\
    option("path","abfss://silver@datasourceazure.dfs.core.windows.net/FactStream/data").\
    toTable("spotify_cata.silver.FactStream")


# COMMAND ----------

