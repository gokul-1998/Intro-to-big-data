# Assignment 4

```
Write PySpark code to implement SCD Type II on a customer master data frame. You will have to create the input files by yourself. For this, use the input data shown as the example in the lecture when this topic was discussed. The PySpark code should be executed on a Dataproc cluster.

Follow the established instructions for submitting your assignment solutions via the below Google form.

Note that within PySpark, there are multiple ways to solve for this problem. Any of the ways is acceptable. But do NOT use SparkSQL.

```

# Solution
 
    - SCD Type II (SCD -> Source, Change, Delete), Type 2 is the same as Type 1, but it also includes the Delete operation
        - Type 1: Source, Change
        - Type 2: Source, Change, Delete

    - PySpark code to implement SCD Type II on a customer master data frame