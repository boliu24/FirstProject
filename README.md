# FirstProject

## Table of Contents

- [Description](#description)
- [Functions](#functions)

## Description
This project's intended purpose is to filter, reorganize, and union yelp data imported to DBFS and transform the dataset into bronze and silver level data tables with 2 easy to use functions.

## Functions
### Raw to Bronze
```python
def raw_to_bronze():
```
the function is without parameters. Running it simply outputs a bronze level parquet file to the DBFS table folder.


```python
reviewschema = StructType([
    StructField("review_id", StringType(), False),   #allowing null
    StructField("user_id", StringType(), False),
    StructField("business_id", StringType(), False),
    StructField("date", DateType(), True),
    StructField("stars", IntegerType(), True),
    StructField("useful", IntegerType(), True),
    StructField("funny", IntegerType(), True),
    StructField("cool", IntegerType(), True),
    StructField("text", StringType(), False),
    ])
```

The function draws columns from 3 tables: review, user and business. A schema is created to match the data type for each json converted df, since inferred schemas are often not ideal.
2 additional tables are dropped due to being deemed irrelevant, and some columns are omitted for the same reason. They could easily be added back should the scope of the project drastically changes.

The json files will then be converted and merged;

```python
review = review.dropDuplicates()
user = user.withColumnsRenamed({"name":"username", "useful":"usefulagg", "funny":"funnyagg", "review_count": "user_review_count"}).dropDuplicates()
business = business.withColumnRenamed("stars", "business_stars").dropDuplicates()
checkin = checkin.dropDuplicates()
```
Duplicate values are dropped in this step, and some columns are renamed for having the same name with other columns in the final table.
I intended to add more filters to each set but could not due to time constraints.

```python
bronze_df.write.format("delta").mode("overwrite").parquet("/FileStore/tables/bronze_data.parquet")
```

Finally, the table will be joined and saved to the path above. The parameters could be manipulated to achieve different outcomes, such as different compression modes and output paths.
The saved parquet file would be a somewhat workable table without duplicates and nonsensical values. 

### Bronze to Silver
```python
def bronze_to_silver():
```
similarly, the function is parameterless and outputs to the same path as the raw to bronze function.
A schema was again being created in order to rearrange the columns to add more readability. The columns are now grouped by the respective tables they came from, and ordered by their relevance.

Presumably, the DA team would want to find out the quality of both individual reviews and the users. To align with this business objective, a few new aggregate columns were added.

```python
sum_df = bronze_df.withColumn("complimentagg", col("usefulagg") + col("funnyagg"))
df = sum_df.withColumn("usefuluser", when(col("complimentagg") > col("user_review_count"), 1).otherwise(0)).drop("complimentagg")
```
The code above generates a column that shows if the user's review count is smaller than the compliments their reviews received. The ratio is arbitrary and could be adjusted once more studies are done.
The idea is to determine a user's basic trustworthiness. If needed, it could be a filter in a query.

```python
window_spec = Window.partitionBy('business_id', 'user_id')
df = df.withColumn('user_reviews_count', count('*').over(window_spec))
df = df.withColumn("multiplereviews", when(col("user_reviews_count") > 1, 1).otherwise(0)).drop("user_reviews_count")
```
The code above generate a signal to specific if a user reviewed a business multiple times, and whether the review is one of duplicated reviews.

```python
df = df.withColumn('days_diff', datediff('date', 'yelping_since'))
silver_df = df.withColumn('years_diff', expr('days_diff / 365')).drop("days_diff")
```
The code above adds a new column that indicates the time passed between the user's registration and the time of the reviewed. The unit is year though it could be changed.
It could be an auxiliary filter to couple with other measurements to determine a user's trustworthiness. 
