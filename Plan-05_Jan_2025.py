	Plan for the day:
*****************






 
5). **monotonically_increasing_id()**: Generate unique IDs.  

The monotonically_increasing_id() function generates a unique, monotonically increasing identifier for each row in a DataFrame. The IDs are guaranteed to be unique but not consecutive. This function is useful when you need to add a unique identifier to rows, such as a surrogate key.


6) Salting Technique:
	Salting methodology:

		from pyspark.sql import functions as F
		df1 = df1.withColumn("salt", F.rand())
		df2 = df2.withColumn("salt", F.rand())
		df_joined = df1.join(df2, (df1.user_id == df2.user_id) & (df1.salt == df2.salt))
		

			You have a large dataset with skewed data. How would you optimize the joins to avoid performance issues?


		from pyspark.sql import SparkSession
		from pyspark.sql.functions import concat, col, lit, floor, rand

		# Initialize Spark session
		spark = SparkSession.builder \
			.appName("Skewed Join Handling with Salting") \
			.getOrCreate()

		# Example data for DataFrame df_a (skewed data)
		df_a = spark.createDataFrame(
			[(1000, "value1")] * 1000 + [(2000, "value2")] * 5,  # ID 1000 skewed
			["ID", "value_a"]
		)

		# Example data for DataFrame df_b (non-skewed data)
		df_b = spark.createDataFrame(
			[(1000, "value3")] * 10 + [(2000, "value4")] * 5,  # ID 1000 repeated fewer times
			["ID", "value_b"]
		)

		# Step 1: Apply Salting to df_a
		# Adding a random number as salt to the "ID" to distribute skewed data
		df_a_salted = df_a.withColumn("partition_id", floor(rand() * 10)) \
						  .withColumn("cmn", concat(col("ID"), lit("_"), col("partition_id")))

		# Step 2: Apply Salting to df_b
		# Adding the same salt logic to df_b to match the salted keys
		df_b_salted = df_b.withColumn("partition_id", floor(rand() * 10)) \
						  .withColumn("cmn", concat(col("ID"), lit("_"), col("partition_id")))

		# Step 3: Perform the salted join
		# Perform the join based on the newly created "cmn" column
		skew_join = df_a_salted.join(df_b_salted, how='inner', on="cmn")

		# Show the results
		skew_join.show()


6). **cube() / rollup()**: Multi-level aggregations. 
7). **fold()**: Aggregate with an initial value.  
8). **approxQuantile()**: Calculate approximate percentiles.
