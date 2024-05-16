import os
import sys
import time

from pyspark import SparkConf, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, MapType, ArrayType
from pyspark.sql.functions import explode, concat_ws, desc
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import udf
from pyspark.sql.functions import col

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import col, max

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark_conf = SparkConf().setAppName("RouteComparing-group15").setMaster("local[*]").set("spark.executor.memory", '4g').set("spark.driver.memory", '15g')
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

print(spark.sparkContext.getConf().get("spark.memory.fraction"))

# route_mapping = spark.read.format("csv").option("header", "true").load("./data/route_mapping.csv")

start_time = time.perf_counter()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("route", ArrayType(
        StructType([
            StructField("from", StringType(), True),
            StructField("to", StringType(), True),
            StructField("merchandise", MapType(StringType(), IntegerType()), True)
        ])
    ), True)
])

# Read the JSON data with the specified schema
actual_routes = spark.read.option("inferSchema", True).json("./data/actual_routes.json", schema=schema, multiLine=True)
standard_routes = spark.read.json("./data/standard_routes.json", schema=schema, multiLine=True)

# Show the DataFrame
actual_routes.show(truncate=False)
standard_routes.show(truncate=False)

## Van iedere from en to feature van berekenen

## ACTUAL

# Step 4: Explode the 'route' column to get separate rows for each route segment
exploded_data = actual_routes.select("id", explode("route").alias("route"))

# Step 5: Concatenate 'from' and 'to' values into a single column
exploded_data = exploded_data.withColumn("from_to", concat_ws("-", "route.from", "route.to"))

# Step 6: Apply StringIndexer to convert 'from_to' into numeric indices
indexer = StringIndexer(inputCol="from_to", outputCol="from_to_index")
indexed_data = indexer.fit(exploded_data).transform(exploded_data)

# Step 7: Assemble features vector
assembler = VectorAssembler(inputCols=["from_to_index"], outputCol="features")
actual_feature_data = assembler.transform(indexed_data)

indexed_data.filter(indexed_data.id == 4).show()

### STANDARD ROUTES

# Step 4: Explode the 'route' column to get separate rows for each route segment
exploded_data = standard_routes.select("id", explode("route").alias("route"))

# Step 5: Concatenate 'from' and 'to' values into a single column
exploded_data = exploded_data.withColumn("from_to", concat_ws("-", "route.from", "route.to"))

# Step 6: Apply StringIndexer to convert 'from_to' into numeric indices
indexer = StringIndexer(inputCol="from_to", outputCol="from_to_index")
indexed_data = indexer.fit(exploded_data).transform(exploded_data)

indexed_data.filter(indexed_data.id == 9).show()
# Step 7: Assemble features vector
standard_feature_data = assembler.transform(indexed_data)



exploded_actual = actual_routes.select("id", explode("route").alias('route')).select('id', 'route').withColumn(
    "from_to", concat_ws("-", "route.from", "route.to"))
actual_routes_feature = exploded_actual.groupBy("id").agg(
    collect_list("from_to").alias("from_to")
).orderBy("id")

# Display the merged dataframe
actual_routes_feature.show(truncate=False)

exploded_standard = standard_routes.select("id", explode("route").alias('route')).select('id', 'route').withColumn(
    "from_to", concat_ws("-", "route.from", "route.to"))
standard_routes_feature = exploded_standard.groupBy("id").agg(
    collect_list("from_to").alias("from_to")
).orderBy("id")



def jaccard_similarity(arr1, arr2):
    max_score = 0

    for i in range(len(arr1)):
        for j in range(len(arr2)):
            if arr1[i] == arr2[j]:
                score = 1
                k = 1

                while i + k < len(arr1) and j + k < len(arr2):
                    if arr1[i + k] == arr2[j + k]:
                        score += 1
                    else:
                        break
                    k += 1

                if score > max_score:
                    max_score = score

    return float(max_score / len(set(arr1).union(set(arr2))))


jaccard_similarity_udf = udf(jaccard_similarity)

# Calculate the cosine similarity between feature sequences
df = actual_routes_feature.alias("df1").crossJoin(standard_routes_feature.alias("df2")) \
    .select(col("df1.id").alias("actual_id"), col("df2.id").alias("standard_id"),
            col("df1.from_to").alias("actual_route"), col("df2.from_to").alias("standard_route")) \
    .withColumn("jaccard_similarity", jaccard_similarity_udf("actual_route", "standard_route")) \
    .select("actual_id", "standard_id", "jaccard_similarity", "actual_route", "standard_route")


filtered = df.filter(df.actual_id == 1).orderBy(desc("jaccard_similarity"))
filtered.show(10, truncate=False)


# Step 1: Read the route_mapping.csv file
spark = SparkSession.builder.getOrCreate()
route_mapping_df = spark.read.csv('./data/route_mapping.csv', header=True)

# Step 2: Join the results DataFrame with route_mapping_df
joined_df = df.join(route_mapping_df, (df.actual_id == route_mapping_df.actual_route_id), how='inner')

# Step 3: Filter rows with the highest Jaccard similarity
# max_sim_df = joined_df.groupBy('actual_id').agg(max('jaccard_similarity').alias("max_jaccard_similarity"))

# result_df = joined_df.join(max_sim_df, (joined_df.actual_id == max_sim_df.actual_id) & (joined_df.jaccard_similarity == max_sim_df.max_jaccard_similarity), how='inner')
# max_jaccard_df = joined_df.groupBy('actual_id').agg(joined_df.max(col('jaccard_similarity')).alias('max_jaccard_similarity'))
window_spec = Window.partitionBy('actual_id').orderBy(col('jaccard_similarity').desc())
result_df = joined_df.withColumn('row_number', row_number().over(window_spec))
result_df = result_df.filter(col('row_number') == 1).drop('row_number').orderBy('actual_id')

total_mappings = result_df.count()
correct_mappings = result_df.filter((col('actual_id') == col('actual_route_id')) & (col('standard_id') == col('standard_route_id'))).count()
print("Number of correctly assigned IDs:", correct_mappings)
accuracy = correct_mappings / total_mappings
print(f"Accuracy on the whole DataFrame: {accuracy * 100:.2f}%")

print(time.perf_counter() - start_time)

# result_df.show()
# joined_df.show()