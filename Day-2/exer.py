# %%
"""
## Basic Structured Operations

"""

# %%
"""
### Step 1: Initialize PySpark Session

"""

# %%
#import findspark
#findspark.init()
import pyspark
from pyspark.sql import SparkSession


# Create a Spark sessionoccupation.createOrReplaceTempView("occupation_view_2")

# spark = SparkSession.builder.appName("day2")\
# .config("spark.driver.bindAddress","localhost")\
# .config("spark.ui.port","4050")\
# .config("spark.eventLog.enabled","true")\
# .config("spark.history.fs.logDirectory","/tmp/spark-events")\
# .getOrCreate()

spark = SparkSession.builder \
    .appName("day2") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port", "4050") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .config("spark.history.fs.logDirectory", "/tmp/spark-events") \
    .getOrCreate()


# %%
"""
### Step 2: Load the Dataset

"""

# %%
# Load the Chipotle dataset into a Spark DataFrame
data_path = "../data/occupation.csv"  # Replace with the actual path
occupation = spark.read.csv(data_path, header=True, inferSchema=True)





# %%
occupation.printSchema()

# %%
"""
### Problem 1: Selecting Specific Columns
Problem: Select the "user_id," "age," and "occupation" columns from the occupation DataFrame.
"""

# %%

print(occupation.columns)
result = occupation.select('user_id','age','occupation')
result.show()

# %%
"""
### Problem 2: Filtering Rows based on Condition
Problem: Find the users who are older than 30 years from the occupation DataFrame.
"""

# %%
from pyspark.sql.functions import col,when,lit
result = occupation.select('*').filter(col('age') > 30)
result.show()


# %%
"""
### Problem 3: Counting and Grouping
Problem: Count the number of users in each occupation from the occupation DataFrame.
"""

# %%
result = occupation.groupBy('occupation').count()

result.show()

# %%
"""
### Problem 4: Adding a New Column
Problem: Add a new column "age_group" to the occupation DataFrame based on the age of the users. Divide users into age groups: "18-25", "26-35", "36-50", and "51+".
"""

# %%
occupation = occupation.withColumn("age_group",
                                 when( (col('age')>18) & (col('age')<=25)  ,"18-25")
                                .when((col('age')>25) &  (col('age')<=35) ,"26-35" ) 
                                .when((col('age')>36) &  (col('age')<=50),"36-50" )
                                .when((col('age')>50),  "51+"))

# %%
#for future use
occupation.createGlobalTempView("occupation_glob")

# %%
occupation.show()

# %%
"""
### Problem 5: Creating DataFrames and Converting to Spark Types
Problem: Given the provided code snippet, create a DataFrame df using the given data and schema. The schema includes columns for firstname, middlename, lastname, id, gender, and salary. After creating the DataFrame, print its schema and display its content without truncation.
"""

# %%
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# %%
 

#Define the schema using StructType and StructField
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(),True),
    StructField("gender", StringType(),True),
    StructField("salary",IntegerType(),True)
])

# Create a list of tuples representing the data
data = [
    ("James", "", "Smith","36636","M",3000),
    ("Michael", "Rose", "","40028","M",4000),
    ("Robert", "", "Williams","42114","M",4000),
    ("Maria", "Anne", "Jones","39192","F",4000),
    ("Jen", "Mary", "Brown","","F",-1)
]

# Create a DataFrame from the data and the defined schema
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.printSchema()

# %%
"""
### Problem 6: Adding and Renaming Columns
Problem: Add a new column "gender" to the existing DataFrame and rename the "Age" column to "Years".
"""

# %%
#this view is used in addditional problems
occupation.createOrReplaceTempView("occupation_view_2")

occupation = occupation.withColumn('gender',lit("Unknown"))
occupation = occupation.withColumnRenamed('Age',"Years" )


# %%
occupation.show()

# %%
"""
### Problem 7: Filtering Rows and Sorting
Problem: Filter out users who are younger than 30 years and sort the DataFrame by age in descending order.
"""

# %%
occupation = occupation.filter(col('Years')>30).orderBy('Years',ascending = False)

# %%
occupation.show()

# %%
"""
### Problem 8: Repartitioning and Collecting Rows
Problem: Repartition the DataFrame into 2 partitions without shuffling the data, then collect and display all rows in the driver and print number of partitions
"""

# %%
df_repart= df.repartition(2)
rows = df_repart.collect()

for row in rows:
    print(row)


# %%
print ("Number of Partitions: ",df_repart.rdd.getNumPartitions())

# %%
"""

"""

# %%
"""
### Additional questions:

Use both spark SQL and Pyspark to obtain answer wherever relevant
"""

# %%
"""
#### Filter out rows where the age is greater than 30 and create a new DataFrame. Then, add a new column named "is_elderly" with a value of "True" for these rows and "False" otherwise.Rename the "gender" column to "sex".
"""

# %%
# Spark SQL
occupation.createOrReplaceTempView("occupation_view")
filtered_occupation =spark.sql("select * from occupation_view where years>30 ") 
filtered_occupation = filtered_occupation.withColumnRenamed("gender","sex")
filtered_occupation.show()

# %%
filtered_occupation= filtered_occupation.withColumn("is_elderly",col('Years')>30)
filtered_occupation.show() 

# %%
# Pyspark
filtered_occ = occupation.filter(col("age") > 30)
filtered_occ = filtered_occ.withColumnRenamed("gender","sex")
filtered_occ = filtered_occ.withColumn('is_elderly',col('Years')>30)
filtered_occ.show()




# %%
"""
#### Calculate the average age of male and female users separately. Present the result in a new DataFrame with columns "gender" and "avg_age".
"""

# %%
# Spark SQL

query =""" 
            select gender,
                    avg(age) as avg_age 
            from 
                    occupation_view_2
            group by 
                    gender
                  
"""


updated_df = spark.sql(query)
updated_df.show()


query =""" 
            select gender,
                    avg(age) as avg_age 
            from 
                    occupation_view_2
            group by 
                    gender
                  
"""


updated_df = spark.sql(query)
updated_df.show()

# %%
# Pyspark
#from pyspark.sql.functions import avg
#avg_age_gender = occupation_2.groupBy("gender").agg(avg(col("age")).alias("avg_age"))

# %%
"""
#### Add a new column named "full_name" to the dataset by concatenating the "user_id" and "occupation" columns. Then, rename the "zip_code" column to "postal_code" in the same DataFrame.
"""

# %%
# Spark SQL
occupation.createOrReplaceTempView("occupation_view")
query = """    
        select user_id,
               Years,
               gender,
               occupation,
               age_group, 
              concat(user_id,'-',occupation) as full_name,
              zip_code as postal_code
        from occupation_view
"""

new_df = spark.sql(query)
new_df.show()


# %%
# Pyspark
from pyspark.sql.functions import concat

updated_df = occupation.withColumn("full_name", concat(col("user_id"), col("occupation"))) \
                        .withColumnRenamed("zip_code","postal_code")

updated_df.show()



# %%
"""
#### Filter out rows where occupation is 'technician', select only the "user_id" and "age" columns, and then add a new column "age_diff" that calculates the difference between the user's age and the average age in the dataset.
"""

# %%
# Spark SQL
from pyspark.sql.functions import avg
occupation.createOrReplaceTempView("occupation_view")
query = """  with cte as (  
            select *  ,
                 avg(Years)  over() as avg_years 
                   
            from 
                occupation_view
            where 
                occupation = 'technician'
                )
          select *,
                 round((cast(Years as float) - avg_years),2) as diff
          from 
                 cte
"""

new_df = spark.sql(query)
new_df.show()


# %%
# Pyspark
filtered_df = occupation.filter(col("occupation") == "technician")

avg_age = occupation.select(avg("Years")).first()[0]

filtered_df = filtered_df.withColumn("age_diff", col("Years")- avg_age) \
                        .select("user_id","Years","age_diff")

filtered_df.show()

# %%
"""
#### Divide the dataset into two DataFrames: one with male users and another with female users. Repartition both DataFrames to have 2 partitions each. Then, union these two DataFrames together and display the resulting DataFrame.
"""

# %%
query = """   
         select *
         from 
            occupation_view_2
         where 
            gender= 'M'
"""

query2 = """   
         select *
         from 
            occupation_view_2
         where 
            gender= 'F'
"""

df_m= spark.sql(query)
df_f = spark.sql(query2)
df_m.show()


# %%
df_f.show()

# %%
partitioned_df_m = df_m.repartition(2)
partitioned_df_f = df_f.repartition(2)


# %%
union_df = partitioned_df_m.union(partitioned_df_f)
union_df.show()

# %%
"""
#### Create and fill a new DataFrame named user_ratings with columns user_id and rating max 10 column. Both user_data and user_ratings share the user_id column. Combine these two DataFrames to create a new DataFrame that includes user information and their corresponding ratings. Make sure to keep only the users present in both DataFrames.
"""

# %%
schema = StructType([
    StructField("user_id",  IntegerType(), True),
    StructField("user_ratings", IntegerType(), True)
])

# Create a list of tuples representing the data
data = [
   (2,5),
   (5,1),
   (11,6),
   (12,8),
   (15,3),
   (18,7),
   (800,8),
   (847,4),
   (440,9),
   (261,2)
]

# Create a DataFrame from the data and the defined schema
df_ratings = spark.createDataFrame(data, schema)

df_ratings.show()

# %%
df_ratings.createOrReplaceTempView("ratings")

query = """    
           select * 
           from 
                occupation_view_2 as o
            inner join
                ratings as r 
            on
                o.user_id = r.user_id
                
"""

combined_df = spark.sql(query)
combined_df.show()