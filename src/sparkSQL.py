# Databricks notebook source
########################################
# Recitation - PySparkSQL
########################################

# COMMAND ----------

### loading a .csv file
log_mini_df = spark.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/log_mini.csv')

# Notice the type - DataFrame
log_mini_df.describe

# COMMAND ----------

display(log_mini_df)

# COMMAND ----------

# get all session lengths longer than 10 

# filter
longer_than_10_df = log_mini_df.filter(log_mini_df.session_length > 10)

# Use 'select' to get a new dataframe with the specified columns
longer_than_10_df = longer_than_10_df.select("session_length")

# remove duplicates
longer_than_10_df = longer_than_10_df.distinct()

# sort ascending order
longer_than_10_df.sort("session_length").show()



# COMMAND ----------

# aggregations
# import pyspark.sql aggregation function (with underscore at the beginning to avoid collision with python's aggregators)
from pyspark.sql.functions import mean as _mean, max as _max, min as _min, sum as _sum, count as _count, first as _first


# tracks listening count (any time it appeared in user playlist)
display(log_mini_df.groupBy('track_id_clean').agg(_count('track_id_clean').alias('count_of_track_id_clean')).sort('count_of_track_id_clean', ascending=False))

# COMMAND ----------

# for class to implement: find the bounds of 'hour_of_day'

# solution (0-23):
display(log_mini_df.agg(_min('hour_of_day').alias('min_hour_of_day'), _max('hour_of_day').alias('max_hour_of_day')))

# COMMAND ----------

# for class to implement: Do you think there is a significant difference in session length between premium and non-premium users? base you answer

# solution (no):

sessions_length_df = log_mini_df.groupBy('session_id').agg(_count('track_id_clean').alias('count_of_track_id_clean'), _first('premium').alias('premium'))
display(sessions_length_df.groupBy('premium').agg(_mean('count_of_track_id_clean')))


# COMMAND ----------

# loading another df

tf_mini_df = spark.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/tf_mini.csv')

# Notice the type - DataFrame
tf_mini_df.describe


# COMMAND ----------

display(tf_mini_df)

# COMMAND ----------

# (inner) join df's

# prepare df1
actions_df = log_mini_df.select('session_id', 'track_id_clean')
actions_df.show()

# prepare df2
songs_df = tf_mini_df.select('track_id', 'duration', 'release_year')
songs_df.show()

# join by track id
joined_df = actions_df.join(songs_df, actions_df.track_id_clean == songs_df.track_id)
joined_df.show()

# COMMAND ----------

# Adding a new column with values based on other values
songs_df = songs_df.withColumn("duration_minutes", songs_df.duration / 60.0)
songs_df.show()

# COMMAND ----------

# for class to implement: Does songs before 1985 (including) are longer than songs after 1985?

# solution:
