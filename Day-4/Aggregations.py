# %%
"""
##  Aggregations

"""

# %%
"""
### Step 1: Initialize PySpark Session

"""

# %%
import findspark
findspark.init()
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("day3").getOrCreate()


# %%
"""
### Step 2: Load the Dataset

"""

# %%
# Load the Chipotle dataset into a Spark DataFrame
data_path = "../data/US_Crime_Rates_1960_2014.csv"  # Replace with the actual path
US_Crime_Rates_1960_2014_df = spark.read.csv(data_path, header=True, inferSchema=True)

# Load the Chipotle dataset into a Spark DataFrame
data_path = "../data/titanic.csv"  # Replace with the actual path
titanic_df = spark.read.csv(data_path, header=True, inferSchema=True)


# %%
US_Crime_Rates_1960_2014_df.printSchema()

# %%
US_Crime_Rates_1960_2014_df.show()


# %%
"""
### count

Question: How many records are there in the US_Crime_Rates_1960_2014_df DataFrame?
"""

# %%
ccount = US_Crime_Rates_1960_2014_df.count()

print("Number of records: ", ccount)

# %%
#temp view for sql 
US_Crime_Rates_1960_2014_df.createOrReplaceTempView('crime_us')

# %%
#sql

count = spark.sql("""
                      select count(*) as number_of_count
                      from crime_us
                     """)


print("Number of Count:", count.collect()[0][0])

# %%
"""
### countDistinct
Question: How many distinct years are present in the US_Crime_Rates_1960_2014_df DataFrame?
Answer:
"""

# %%
from pyspark.sql.window import Window
from pyspark.sql.functions import col, countDistinct ,approxCountDistinct, first , last , max,min , sumDistinct,avg,struct,sum

# %%

count_year = US_Crime_Rates_1960_2014_df.select(countDistinct('Year'))
print("Number of Distinct Years: ", count_year.collect()[0][0])


# %%
#sql

distinct_count = spark.sql("""
                        select count(distinct 'year') as distinct_count_of_years
                       from crime_us
                         """)   
# distinct_count.show()
print("Number of distinct years:",distinct_count.collect()[0][0])

# %%
"""
### approx_count_distinct

Question: Estimate the approximate number of distinct values in the "Total" column of the US_Crime_Rates_1960_2014_df DataFrame.
"""

# %%
#here we use approxCountDistinct with max estimation error of 0.1
US_Crime_Rates_1960_2014_df.select(approxCountDistinct('Total',0.1)).show()
print("",)


# %%
"""
###  first and last

Question: Find the first and last year in the US_Crime_Rates_1960_2014_df DataFrame.
"""

# %%
first = US_Crime_Rates_1960_2014_df.select(first('Year'))
last  = US_Crime_Rates_1960_2014_df.select(last('Year'))
print("First Year: ", first.collect()[0][0])
print("Last Year: ", last.collect()[0][0])

# %%
"""
### min and max

Question: Find the minimum and maximum population values in the US_Crime_Rates_1960_2014_df DataFrame.
"""

# %%
min = US_Crime_Rates_1960_2014_df.select(min("Population"))
max = US_Crime_Rates_1960_2014_df.select(max("Population"))

print("Minimum Population",min.collect()[0][0])
print("Maximum Population",max.collect()[0][0])

# %%
"""
### sumDistinct

Question: Calculate the sum of distinct "Property" values for each year in the US_Crime_Rates_1960_2014_df DataFrame.
"""

# %%
#in this problem , we first use the groupby clause to group the property value for each year 
# then we use the funcion sumDistinct to do so .
# we can also use the method sum_distinct mehtod because of the future deprecation of the previous function 

distinct_sum = US_Crime_Rates_1960_2014_df.groupBy('Year').agg(sumDistinct(col('Property')).alias('SumDistinctYear'))
distinct_sum.show()

# %%
"""
### avg

Question: Calculate the average "Murder" rate for the entire dataset in the US_Crime_Rates_1960_2014_df DataFrame.
Answer:
"""

# %%
avg_murder = US_Crime_Rates_1960_2014_df.select(avg(col("Murder")))
print("Average Murder Rate: ", avg_murder.collect()[0][0])

# %%
US_Crime_Rates_1960_2014_df.show()

# %%
"""
### Aggregating to Complex Types

Question: Calculate the total sum of "Violent" and "Property" crimes for each year in the US_Crime_Rates_1960_2014_df DataFrame. Store the results in a struct type column.
"""

# %%
distinct_sum = US_Crime_Rates_1960_2014_df.groupBy('Year').agg(
    struct(
    sum(col('Property')),
    sum(col('Violent'))
    ).alias("CrimeSums")
)
distinct_sum.show()


# %%
"""
### Grouping

Question: In the given US_Crime_Rates_1960_2014_df DataFrame, you are tasked with finding the average of all crimes combined for each year. Calculate the sum of all crime categories (Violent, Property, Murder, Forcible_Rape, Robbery, Aggravated_assault, Burglary, Larceny_Theft, Vehicle_Theft) for each year and then determine the average of these combined crime sums. Provide the result as the average of all crimes across the entire dataset.
"""

# %%
# first we calculated the total sum of al the crimes in a year 
# and then we averaged that and we showed as per the expected outcome
sum_crime_year = US_Crime_Rates_1960_2014_df.withColumn(
    'TotalCrimes',
    col("Violent") + col("Property") + col("Murder") + col("Forcible_Rape") + col("Robbery") + col("Aggravated_assault") + col("Burglary") + col("Larceny_Theft") + col("Vehicle_Theft")
)


average_crime = sum_crime_year.select(avg(col("TotalCrimes")))

print("Average of all crimes: ",average_crime.collect()[0][0])

sum_crime_year.select("Year","TotalCrimes").show()

# %%
"""
### Window Functions

Question: Calculate the cumulative sum of "Property" values over the years using a window function in the US_Crime_Rates_1960_2014_df DataFrame.
"""

# %%


# %%
wind_spec = Window.orderBy("Year")
cum_sum_prop_df = US_Crime_Rates_1960_2014_df.withColumn(
    "CumulativePropertySum",
    sum(col("Property")).over(wind_spec)
)

cum_sum_prop_df.show()

# %%
"""
### Pivot
Question: You are working with a DataFrame named US_Crime_Rates_1960_2014_df that contains crime data for different crime types over the years. 
"""

# %%
#this gives us the result as expected. 
pivot_df = US_Crime_Rates_1960_2014_df.groupBy("Year").pivot("Year").agg({"Robbery": "max"})
pivot_df.orderBy('Year').show()

# %%


# %%
