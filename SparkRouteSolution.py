import argparse
import os
import sys
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, concat_ws, collect_list, col, format_number, udf, round
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, MapType, ArrayType

import pyspark.sql.functions as F

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

parser = argparse.ArgumentParser(description='Script for generating standard routes.')
parser.add_argument('--standard', help='Path to the standard route json', required=True)
parser.add_argument('--actual', help='Path to actual routes json', required=True)
parser.add_argument('--output', help='Path to the save the output', required=True)
args = parser.parse_args()


def lcs_similarity(actual_route, standard_route, actual_merch, standard_merch, function):
    matrix = [[0] * (len(standard_route) + 1) for _ in range(len(actual_route) + 1)]

    # Fill the matrix
    for i in range(1, len(actual_route) + 1):
        for j in range(1, len(standard_route) + 1):
            if actual_route[i - 1] == standard_route[j - 1]:
                matrix[i][j] = matrix[i - 1][j - 1] + 1
            else:
                matrix[i][j] = max(int(matrix[i - 1][j]), int(matrix[i][j - 1]))

    # read the matrix
    i = len(actual_route)
    j = len(standard_route)

    matching_indices = []

    while i > 0 and j > 0:
        if actual_route[i - 1] == standard_route[j - 1]:
            matching_indices.append((i - 1, j - 1))
            i -= 1
            j -= 1
        elif matrix[i - 1][j] > matrix[i][j - 1]:
            i -= 1
        else:
            j -= 1

    matching_indices.reverse()

    diff_merch = 0.01
    total_merch = 1

    for i, j in matching_indices:
        for merch in actual_merch[i]:
            total_merch += 1  # count total products in actual
            if merch in standard_merch[j]:
                diff_merch += 1  # count same products
                diff_merch -= abs(actual_merch[i][merch] - standard_merch[j][merch]) / actual_merch[i][merch]  # penalize difference proportionally

    if function == 'similarity':  ## SIMILARITY
        lcs = float(matrix[-1][-1] / len(set(actual_route).union(set(standard_route))))
        merch = diff_merch / total_merch
        value = lcs + merch

    elif function == 'cost':  ## COST
        route_cost = float(matrix[-1][-1]) * 1000 - (len(standard_route) - float(matrix[-1][-1])) * 500
        product_cost = -50 * diff_merch
        value = route_cost + product_cost

    return value


spark_conf = SparkConf() \
    .setAppName("RouteComparing-group15") \
    .setMaster("local[*]") \
    .set("spark.executor.memory", "3g") \
    .set("spark.driver.memory", "22g")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

startTime = time.perf_counter()

routeSchema = StructType([
    StructField("id", IntegerType(), False),
    StructField("route", ArrayType(
        StructType([
            StructField("from", StringType(), False),
            StructField("to", StringType(), False),
            StructField("merchandise", MapType(StringType(), IntegerType()), False)
        ])
    ), False)
])

lcs_similarity_function = udf(lambda actual_route, standard_route,
                                     actual_merchandise, standard_merchandise,
                                     flag: lcs_similarity(actual_route,
                                                          standard_route,
                                                          actual_merchandise,
                                                          standard_merchandise,
                                                          flag), StringType())

standard_routes = spark.read.option("multiLine", True).schema(routeSchema).json(args.standard)
actual_routes = spark.read.option("multiLine", True).schema(routeSchema).json(args.actual)

exploded_actual = actual_routes.select("id", explode("route").alias('route')).select('id', 'route').withColumn(
    "from_to", concat_ws("-", "route.from", "route.to"))
exploded_actual = exploded_actual.withColumn("merchandise", exploded_actual.route.merchandise)

actual_routes_feature = exploded_actual.groupBy("id").agg(
    collect_list("from_to").alias("from_to"),
    collect_list("merchandise").alias("merchandise")
)

#actual_routes_feature.show(truncate=False)

exploded_standard = standard_routes.select("id", explode("route").alias('route')).select('id', 'route')
exploded_standard = exploded_standard.withColumn("from_to", concat_ws("-", "route.from", "route.to")) \
    .withColumn("merchandise", exploded_standard.route.merchandise)

standard_routes_feature = exploded_standard.groupBy("id").agg(
    collect_list("from_to").alias("from_to"),
    collect_list("merchandise").alias("merchandise"))

#standard_routes_feature.show(truncate=False)

actual_routes_seperated = actual_routes_feature.select('id', explode("from_to").alias('trips'))

# Group actual_routes by trips and collect a list of IDs for each trips
actual_routes_nodes = actual_routes_seperated \
    .groupBy("trips") \
    .agg(collect_list('id').alias("ids"))

df_exploded_cities = standard_routes_feature.select(col('id').alias('standard_id'),
                                                    explode(standard_routes_feature['from_to']).alias('from_to_exploded'))

#df_exploded_cities.show(truncate=False)

# match exploded cities with created mapping for id's
df_joined = df_exploded_cities.join(actual_routes_nodes, df_exploded_cities['from_to_exploded'] == actual_routes_nodes['trips'])

# df_joined.show(truncate=False)

# explode all id's
df_exploded_id = df_joined.withColumn("actual_id", explode(df_joined["ids"]).alias('actual_id'))\
    .select("standard_id", col("from_to_exploded").alias("standard_city"), "actual_id", col("trips").alias("actual_city"))

# join based on id with standard and actual for their merchandise and route
joined_df = df_exploded_id.join(actual_routes_feature, df_exploded_id.actual_id == actual_routes_feature.id)\
    .withColumnRenamed('merchandise', 'actual_merchandise')\
    .withColumnRenamed('from_to', 'actual_route').cache()

joined_df = joined_df.join(standard_routes_feature,
                           df_exploded_id.standard_id == standard_routes_feature.id)\
    .withColumnRenamed('merchandise', 'standard_merchandise')\
    .withColumnRenamed('from_to', 'standard_route')\
    .drop(*['id']).cache()

####################################### similarity scores

similarity_df = joined_df.withColumn("similarity", lcs_similarity_function("actual_route", "standard_route", "actual_merchandise",
                                                           "standard_merchandise", F.lit('similarity'))) \
    .select("actual_id", "standard_id", format_number(col("similarity").cast("double"), 10).alias('similarity'),
            "actual_route", "standard_route", "actual_merchandise", "standard_merchandise")

####################################### max for every actual id and standard id (unique combinations)

max_similarity_df = similarity_df.groupBy("actual_id").agg(F.max("similarity").alias("max_similarity")).cache()

result_df = similarity_df.join(max_similarity_df,
                               (similarity_df.actual_id == max_similarity_df.actual_id) & (similarity_df.similarity == max_similarity_df.max_similarity), "inner")\
    .drop(max_similarity_df.actual_id)


result_df = result_df.dropDuplicates(["actual_id", "standard_id"])
result_df = result_df.repartition(1000)

result_df.select('actual_id', 'standard_id').toPandas().to_csv(args.output, index=False)

print(f"done in {time.perf_counter() - startTime}")


payment_df = actual_routes_feature.filter(actual_routes_feature.id < 1000)
payment_df = payment_df.join(result_df, payment_df.id == result_df.actual_id)
payment_df = payment_df.withColumn("cost", lcs_similarity_function("from_to", "standard_route", "actual_merchandise", "standard_merchandise", F.lit('cost'))) \
    .select("actual_id", "standard_id", round(col("cost").cast("double"), 0).alias('cost'),"from_to", "standard_route", "actual_merchandise","standard_merchandise")
# payment_df.show()


costs_list = payment_df.select(col("cost").cast("int")).rdd.flatMap(lambda x: x).collect()


def knapsack(costs, budget):
    n = len(costs)
    dp = [[0] * (budget + 1) for _ in range(n + 1)]

    # fill in matrix
    for i in range(1, n + 1):
        for j in range(1, budget + 1):
            if costs[i - 1] <= j:
                dp[i][j] = max(dp[i - 1][j], dp[i - 1][j - costs[i - 1]] + costs[i - 1])
            else:
                dp[i][j] = dp[i - 1][j]

    # select optimal indices
    indices = []
    i, j = n, budget
    while i > 0 and j > 0:
        if dp[i][j] != dp[i - 1][j]:
            indices.append(i - 1)
            j -= costs[i - 1]
        i -= 1

    # remove optimal indices backwards
    for index in sorted(indices, reverse=True):
        if index < len(costs):
            costs.pop(index)

    return costs, indices, budget - dp[n][budget]


def linear_payments(costs):
    remaining_budget = 495000

    for cost in costs.copy():
        if cost in costs:
            if cost <= remaining_budget:
                remaining_budget -= cost
                costs.remove(cost)
            else:
                break

    costs, indices, penalty = knapsack(costs, 5000 + remaining_budget)

    return costs, indices, penalty


total_penalty = 0
use_costs = costs_list.copy()

while len(use_costs) > 0:
    use_costs, indices, penalty = linear_payments(use_costs)
    total_penalty += penalty
    # use_costs = list(filter(lambda x: x not in selected_costs, use_costs))

    print("Selected Costs:", indices)
    print("Penalty:", penalty)

print("#####")
print("Costs:", use_costs)
print("Penalty:", total_penalty)

