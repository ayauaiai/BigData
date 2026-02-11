
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, avg, sum as sum_
import time


spark = SparkSession.builder \
    .appName("PySpark_Labs") \
    .master("local[*]") \
    .getOrCreate()


data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Ayau", 26),
    (4, "Aru", 22),
    (5, "Brin", 32),
    (6, "Alisa", 74),
    (7, "Charlie", 12)
]
columns = ["id", "name", "age"]

df = spark.createDataFrame(data, schema=columns)
print("Lab 1 Creating a Spark DataFrame")
df.show()


print("Lab 2 Counting Records")
total_count = df.count()
print(f"Total rows: {total_count}")

count_name = df.select("name").count()
count_age = df.select("age").count()
print(f"Rows after selecting 'name': {count_name}")
print(f"Rows after selecting 'age': {count_age}")

count_subset = df.select("name", "age").count()
print(f"Rows after selecting 'name' and 'age': {count_subset}")


print("Lab 3 Filtering Data")
filtered_col = df.filter(col("age") > 21)
filtered_col.show()

filtered_sql = df.filter("age > 21")
filtered_sql.show()

name_filter = df.filter("name LIKE 'B%'")
name_filter.show()


print("Lab 4 Adding Columns")
df_modified = df.withColumn("country", lit("USA"))
df_modified.show()


print("Lab 5 Aggregations")
grouped = df.groupBy("name").count()
grouped.show()

global_avg_age = df.select(avg("age")).collect()[0][0]
print(f"Global average age: {global_avg_age}")


print("Lab 6 Reading CSV Files")

csv_path = "people.csv" 
df_spark = spark.read.csv(csv_path, header=True, inferSchema=True)
df_spark.show()
df_spark.printSchema()


print("Lab 7 Basic Data Cleaning")
null_counts = df_spark.select([sum_(col(c).isNull().cast("int")).alias(c) for c in df_spark.columns])
print("Null values per column:")
null_counts.show()

dropped = df_spark.dropna()
print("DataFrame after dropping nulls:")
dropped.show()


print("Lab 8 Spark SQL")
df_spark.createOrReplaceTempView("people")

sql_result = spark.sql("SELECT * FROM people WHERE age > 21")
print("People with age > 21:")
sql_result.show()

avg_age = spark.sql("SELECT AVG(age) AS avg_age FROM people")
print("Average age:")
avg_age.show()

names_only = spark.sql("SELECT name FROM people")
print("Names only:")
names_only.show()

print("Lab 9 Lazy Evaluation Demonstration")

lazy_filtered = df_spark.filter(col("age") > 30)
lazy_filtered.show()

print("Lab 10 When Not to Use Spark")

py_data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Ayau", 26),
    (4, "Aru", 22),
    (5, "Brin", 32),
    (6, "Alisa", 74),
    (7, "Charlie", 12)
]

start_py = time.time()
avg_age_py = sum([x[2] for x in py_data]) / len(py_data)
end_py = time.time()
print(f"Python average age: {avg_age_py}")
print(f"Execution time (Python): {end_py - start_py:.6f} sec")

start_spark = time.time()
avg_age_spark = df.select(avg("age")).collect()[0][0]
end_spark = time.time()
print(f"Spark average age: {avg_age_spark}")
print(f"Execution time (Spark): {end_spark - start_spark:.6f} sec")