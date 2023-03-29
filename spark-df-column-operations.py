from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("sample-ap").getOrCreate()

emp_data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]
columns = ["firstname","lastname","country","state"]

df = spark.createDataFrame(data=emp_data,schema=columns)
df.show(truncate=False)


# Prepare data
sales_data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4600), \
    ("James", "Sales", 3000)
  ]
columns= ["employee_name", "dept", "salary"]# Prepare data

df = spark.createDataFrame(data=sales_data, schema=columns)
df.printSchema()

print("Original Df")
df.show(truncate=False)

print("Distinct Df")
distinct_df = df.distinct()
distinct_df.show()

print("Drop duplicates Df by dept and salary")
drop_dup_df = df.drop_duplicates(["dept", "salary"])
drop_dup_df.show(truncate=False)
