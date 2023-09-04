# %%
"""
## Joins
"""

# %%

import findspark 
findspark.init()
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("day3").getOrCreate()


# %%
"""

Question: You are given two DataFrames: employees_df and departments_df, which contain information about employees and their respective departments. The schema for the DataFrames is as follows:

employees_df schema:
|-- employee_id: integer (nullable = true)
|-- employee_name: string (nullable = true)
|-- department_id: integer (nullable = true)

departments_df schema:

|-- department_id: integer (nullable = true)
|-- department_name: string (nullable = true)

Employees DataFrame:
                                                                                
+-----------+-------------+-------------+
|employee_id|employee_name|department_id|
+-----------+-------------+-------------+
|1          |Pallavi mam  |101          |
|2          |Bob          |102          |
|3          |Cathy        |101          |
|4          |David        |103          |
|5          |Amrit Sir    |104          |
|6          |Alice        |null         |
|7          |Eva          |null         |
|8          |Frank        |110          |
|9          |Grace        |109          |
|10         |Henry        |null         |
+-----------+-------------+-------------+



Departments DataFrame:
+-------------+------------------------+
|department_id|department_name         |
+-------------+------------------------+
|101          |HR                      |
|102          |Engineering             |
|103          |Finance                 |
|104          |Marketing               |
|105          |Operations              |
|106          |null                    |
|107          |Operations              |
|108          |Production              |
|null         |Finance                 |
|110          |Research and Development|
+-------------+----------------------

"""

# %%
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("employee_name", StringType(), True),
    StructField("department_id", IntegerType(), True)
])

data = [
    (1, "Pallavi mam", 101),
    (2, "Bob", 102),
    (3, "Cathy", 101),
    (4, "David", 103),
    (5, "Amrit Sir", 104),
    (6, "Alice", None),
    (7, "Eva", None),
    (8, "Frank", 110),
    (9, "Grace", 109),
    (10, "Henry", None)
]

employees_df = spark.createDataFrame(data, schema=schema)
schema = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("department_name", StringType(), True)
])

data = [
    (101, "HR"),
    (102, "Engineering"),
    (103, "Finance"),
    (104, "Marketing"),
    (105, "Operations"),
    (106, None),
    (107, "Operations"),
    (108, "Production"),
    (None, "Finance"),
    (110, "Research and Development")
]

departments_df = spark.createDataFrame(data, schema=schema)
employees_df.printSchema()
employees_df.show()
departments_df.printSchema()
departments_df.show()



# %%
#CREATE TEMP TAVLE FOR EMPLOYEE AND DEPARTMENT
departments_df.createOrReplaceTempView("department")
employees_df.createOrReplaceTempView("employee")

# %%
"""
### Join Expressions

Question: How can you combine the employees_df and departments_df DataFrames based on the common "department_id" column to get a combined DataFrame with employee names and their respective department names?
"""

# %%
# this can be done using the join as given below. 

join_exp = employees_df['department_id'] == departments_df['department_id']
result_df = employees_df.join(departments_df,join_exp).select(departments_df['department_id'],'employee_id','employee_name','department_name')
result_df.show()

# %%
#sql

join_sql = spark.sql("""
                    select d.department_id, e.employee_id, e.employee_name, d.department_name
                    from department d
                    join employee e
                    on d.department_id = e.department_id
""")
                     
join_sql.show()

# %%
"""
### Inner Joins

Question: How can you retrieve employee names and their respective department names for employees belonging to the "Engineering" department?
"""

# %%
#we can do inner join to perform this 
# we use select method in order to select the required columns
#here , we have used the where clause in order to show the department name of employee bob as shown in the notebook 
join_type = 'inner'
join_exp2 = employees_df['department_id']==departments_df['department_id']

employees_df.join(departments_df,join_exp2,join_type).select('employee_name','department_name')\
    .where(departments_df['department_name']=='Engineering').show()

# %%
"""
### Outer Joins

Question: Retrieve a DataFrame that contains all employees along with their department names. If an employee doesn't have a department assigned, display "No Department".
"""

# %%
#we can do this using the  full outer joins , 
#here , if we use full outer join then we also get to see the null values getting replaced with 'no department' in the employee_name column
#we use na.fill method in order to fill the null values with "no department" for the whole table as shown i expected output
join_type = 'outer'
# we areusing join expression of above because of the same keys 

employees_df.join(departments_df,join_exp,join_type)\
            .select('employee_name',departments_df['department_name'])\
            .na.fill('No Department')\
            .show()

# %%
"""
### Left Outer Joins

Question: List all employees along with their department names. If an employee doesn't have a department assigned, display "No Department".
"""

# %%
# we can solve this problem using the left jon mehtod given below

join_type = 'left_outer'

employees_df.join(departments_df,join_exp,join_type)\
            .select('employee_name',departments_df['department_name'])\
            .na.fill('No Department')\
            .show()

# %%
"""
### Right Outer Joins

Question: Display a list of departments along with employee names. If a department has no employees, display "No Employees".


"""

# %%
# we can do this as follows 

join_type = 'right_outer'

employees_df.join(departments_df, join_exp,join_type)\
            .select(departments_df['department_name'], 'employee_name')\
            .na.fill('No Employee')\
            .show()

# %%
"""
### Left Semi Joins

Question: Retrieve a DataFrame that includes employee names for departments that have employees.


"""

# %%
# we can do this using left semi join to check wherether the employee in the left table has /doesnthave deparment value in the right table

join_type = 'left_semi'

employees_df.join(departments_df,join_exp,join_type)\
            .select('employee_name')\
            .show()

# %%
"""
### Left Anti Joins

Question: Find the employees who don't belong to any department.
"""

# %%
join_type='left_anti'

employees_df.join(departments_df,join_exp,join_type)\
            .select('employee_name')\
            .show()

# %%
"""
### Cross (Cartesian) Joins

Question: Create a DataFrame that contains all possible combinations of employees and departments.
"""

# %%
employees_df.crossJoin(departments_df).show()

# %%
