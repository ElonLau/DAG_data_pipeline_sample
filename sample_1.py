

# Ingesting Data
# Creating a data transformation pipeline with PySpark


# Import json
import json

database_address = {
  "host": "10.0.0.5",
  "port": 8456
}

# Open the configuration file in writable mode, mode = 'w'
with open("database_config.json", mode="w") as file_head:


  # Serialize the object in this file handle
  json.dump(obj=database_address, fp=file_head)

  # Complete the JSON schema
schema = {'properties': {
    'brand': {'type': 'string'},
    'model': {'type': 'string'},
    'price': {'type': 'number'},
    'currency': {'type': 'string'},
    'quantity': {'type': 'integer', 'minimum': 1},
    'date': {'type': 'string', 'format': 'date'},
    'countrycode': {'type': 'string', 'pattern': "^[A-Z]{2}$"},
    'store_name': {'type': 'string'}}}

# Write the schema
singer.write_schema(stream_name='products', schema=schema, key_properties=[])

"""

<script.py> output:
    {"type": "SCHEMA", "stream": "products", "schema":

    {"properties": {"brand": {"type": "string"},\
     "model": {"type": "string"}, "price": {"type": "number"}, "currency": {"type": "string"}, "quantity":\
      {"type": "integer", "minimum": 1}, "date": {"type": "string", "format": "date"}, "countrycode": \
      {"type": "string", "pattern": "^[A-Z]{2}$"}, "store_name": {"type": "string"}}}

      # this part is exactly the same as the 'schema'

       , "key_properties": []}

"""



# -----------------------------------------------------------------------

endpoint = "http://localhost:5000"

# Fill in the correct API key
api_key = "scientist007"

# Create the web API’s URL
authenticated_endpoint = "{}/{}".format(endpoint, api_key)      # {endpoint}/{api_key}

# Get the web API’s reply to the endpoint
api_response = requests.get(authenticated_endpoint).json()
pprint.pprint(api_response)

# Create the API’s endpoint for the shops
shops_endpoint = "{}/{}/{}/{}".format(endpoint, api_key, "diaper/api/v1.0", "shops")

# {endpoint}/{api_key}/{"diaper/api/v1.0"}/{"shops"}


shops = requests.get(shops_endpoint).json()
print(shops)

"""
<script.py> output:

    {'apis': [{'description': 'list the shops available',
               'url': '<api_key>/diaper/api/v1.0/shops'},
              {'description': 'list the items available in shop',
               'url': '<api_key>/diaper/api/v1.0/items/<shop_name>'}]}      # api_response


"""


# --------------------------------------------------------------------

# Create the API’s endpoint for items of the shop starting with a "D"
items_of_specific_shop_URL = "{}/{}/{}/{}/{}".format(endpoint, api_key, "diaper/api/v1.0", "items", "DM")

# {endpoint}/{api_key}/{"diaper/api/v1.0"}/{"items"}/{"DM"}


products_of_shop = requests.get(items_of_specific_shop_URL).json()

pprint.pprint(products_of_shop)

"""
{'shops': ['Aldi', 'Kruidvat', 'Carrefour', 'Tesco', 'DM']}

# "{}/{}/{}/{}/{}".format(endpoint, api_key, "diaper/api/v1.0", "items", "DM")

# products_of_shop

{'items': [{'brand': 'Huggies',
            'countrycode': 'DE',
            'currency': 'EUR',
            'date': '2019-02-01',
            'model': 'newborn',
            'price': 6.8,
            'quantity': 40},

           {'brand': 'Huggies',
            'countrycode': 'AT',
            'currency': 'EUR',
            'date': '2019-02-01',
            'model': 'newborn',
            'price': 7.2,
            'quantity': 40}]}

"""




# -----------------------------------------------------------------------

# Use the convenience function to query the API
tesco_items = retrieve_products("Tesco")

singer.write_schema(stream_name="products", schema=schema,
                    key_properties=[])

# Write a single record to the stream, that adheres to the schema
singer.write_record(stream_name="products",
                    record={**tesco_items[0], "store_name": "Tesco"})

for shop in requests.get(SHOPS_URL).json()["shops"]:
    # Write all of the records that you retrieve from the API
    singer.write_records(
      stream_name="products", # Use the same stream name that you used in the schema
      records=({**item, "store_name": shop}
               for item in retrieve_products(shop))
    )

"""
<script.py> output:
    {"type": "SCHEMA", "stream": "products", "schema": {"properties": {"brand": {"type": "string"}, "model": {"type": "string"}, "price": {"type": "number"}, "currency": {"type": "string"}, "quantity": {"type": "integer", "minimum": 1}, "date": {"type": "string", "format": "date"}, "countrycode": {"type": "string", "pattern": "^[A-Z]{2}$"}, "store_name": {"type": "string"}}}, "key_properties": []}

    {"type": "RECORD", "stream": "products", "record": {"countrycode": "IE", "brand": "Pampers", "model": "3months", "price": 6.3, "currency": "EUR", "quantity": 35, "date": "2019-02-07", "store_name": "Tesco"}}
    {"type": "RECORD", "stream": "products", "record": {"countrycode": "BE", "brand": "Diapers-R-Us", "model": "6months", "price": 6.8, "currency": "EUR", "quantity": 40, "date": "2019-02-03", "store_name": "Aldi"}}
    {"type": "RECORD", "stream": "products", "record": {"countrycode": "BE", "brand": "Nappy-k", "model": "2months", "price": 4.8, "currency": "EUR", "quantity": 30, "date": "2019-01-28", "store_name": "Kruidvat"}}
    {"type": "RECORD", "stream": "products", "record": {"countrycode": "NL", "brand": "Nappy-k", "model": "2months", "price": 5.6, "currency": "EUR", "quantity": 40, "date": "2019-02-15", "store_name": "Kruidvat"}}
    {"type": "RECORD", "stream": "products", "record": {"store": "Carrefour", "countrycode": "FR", "brand": "Nappy-k", "model": "2months", "price": 5.7, "currency": "EUR", "quantity": 30, "date": "2019-02-06", "store_name": "Carrefour"}}
    {"type": "RECORD", "stream": "products", "record": {"countrycode": "IE", "brand": "Pampers", "model": "3months", "price": 6.3, "currency": "EUR", "quantity": 35, "date": "2019-02-07", "store_name": "Tesco"}}
    {"type": "RECORD", "stream": "products", "record": {"countrycode": "DE", "brand": "Huggies", "model": "newborn", "price": 6.8, "currency": "EUR", "quantity": 40, "date": "2019-02-01", "store_name": "DM"}}
    {"type": "RECORD", "stream": "products", "record": {"countrycode": "AT", "brand": "Huggies", "model": "newborn", "price": 7.2, "currency": "EUR", "quantity": 40, "date": "2019-02-01", "store_name": "DM"}}

"""


# ------------------------------------------------------------------------------

# pyspark starts here

# Read a csv file and set the headers to True
df = (spark.read
      .options(header=True)
      .csv("/home/repl/workspace/mnt/data_lake/landing/ratings.csv"))

df.show()

"""
<script.py> output:
    +------------+-------+---------------+-------+
    |       brand|  model|absorption_rate|comfort|
    +------------+-------+---------------+-------+
    |Diapers-R-Us|6months|              2|      3|
    |     Nappy-k|2months|              3|      4|
    |     Pampers|3months|              4|      4|
    |     Huggies|newborn|              3|      5|
    +------------+-------+---------------+-------+
"""

# Define the schema
schema = StructType([
  StructField("brand", StringType(), nullable=False),
  StructField("model", StringType(), nullable=False),
  StructField("absorption_rate",ByteType() , nullable=True),
  StructField("comfort", ByteType(), nullable=True)
])

better_df = (spark
             .read
             .options(header="true")
             # Pass the predefined schema to the Reader
             .schema(schema)
             .csv("/home/repl/workspace/mnt/data_lake/landing/ratings.csv"))
pprint(better_df.dtypes)

"""
<script.py> output:
    [('brand', 'string'),
     ('model', 'string'),
     ('absorption_rate', 'tinyint'),        # 'tinyint' == ByteType()
     ('comfort', 'tinyint')]

"""

# -----------------------------------------------------------------------------

# Specify the option to drop invalid rows
ratings = (spark
           .read
           .options(header=True, mode="DROPMALFORMED")
           .csv("/home/repl/workspace/mnt/data_lake/landing/ratings_with_invalid_rows.csv"))
ratings.show()


"""

<script.py> output:
    +------------+-------+---------------+-------+
    |       brand|  model|absorption_rate|comfort|
    +------------+-------+---------------+-------+
    |Diapers-R-Us|6months|              2|      3|
    |     Nappy-k|2months|              3|      4|
    |     Pampers|3months|              4|      4|
    |     Huggies|newborn|              3|      5|
    +------------+-------+---------------+-------+

"""
# ----------------------------------------------------------------------------

print("BEFORE")
ratings.show()

print("AFTER")
# Replace nulls with arbitrary value on column subset
ratings = ratings.fillna(4, subset=["comfort"])
ratings.show()


"""
<script.py> output:
    BEFORE
    +------------+-------+---------------+-------+
    |       brand|  model|absorption_rate|comfort|
    +------------+-------+---------------+-------+
    |Diapers-R-Us|6months|              2|      3|
    |     Nappy-k|2months|              3|      4|
    |     Pampers|3months|              4|      4|
    |     Huggies|newborn|              3|      5|
    |     Pampers|    2mo|           null|   null|
    +------------+-------+---------------+-------+

    AFTER
    +------------+-------+---------------+-------+
    |       brand|  model|absorption_rate|comfort|
    +------------+-------+---------------+-------+
    |Diapers-R-Us|6months|              2|      3|
    |     Nappy-k|2months|              3|      4|
    |     Pampers|3months|              4|      4|
    |     Huggies|newborn|              3|      5|
    |     Pampers|    2mo|           null|      4|
    +------------+-------+---------------+-------+

"""




# ----------------------------------

from pyspark.sql.functions import col, when

# Add/relabel the column
categorized_ratings = ratings.withColumn(
    "comfort",
    # Express the condition in terms of column operations
    when(col("comfort") > 3, "sufficient").otherwise("insufficient"))

categorized_ratings.show()

"""


<script.py> output:
    +------------+-------+---------------+------------+
    |       brand|  model|absorption_rate|     comfort|
    +------------+-------+---------------+------------+
    |Diapers-R-Us|6months|              2|insufficient|
    |     Nappy-k|2months|              3|  sufficient|
    |     Pampers|3months|              4|  sufficient|
    |     Huggies|newborn|              3|  sufficient|
    +------------+-------+---------------+------------+


"""


# -----------------------------------------------------------------------------

from pyspark.sql.functions import col

# Select the columns and rename the "absorption_rate" column
result = ratings.select([col("brand"),
                       col("model"),
                       col("absorption_rate").alias("absorbency")])

# Show only unique values
result.distinct().show()

"""
<script.py> output:
    +------------+-------+----------+
    |       brand|  model|absorbency|
    +------------+-------+----------+
    |     Pampers|3months|         4|
    |Diapers-R-Us|6months|         2|
    |     Huggies|newborn|         3|
    |     Nappy-k|2months|         3|
    +------------+-------+----------+
"""

# -----------------------------------------------------------------------------


from pyspark.sql.functions import col, avg, stddev_samp, max as sfmax

aggregated = (purchased
              # Group rows by 'Country'
              .groupBy(col('Country'))
              .agg(
                # Calculate the average salary per group and rename
                avg('Salary').alias('average_salary'),
                # Calculate the standard deviation per group
                stddev_samp('Salary'),
                # Retain the highest salary per group and rename
                sfmax('Salary').alias('highest_salary')
              )
             )

aggregated.show()


"""
<script.py> output:
    +-------+--------------+-------------------+--------------+
    |Country|average_salary|stddev_samp(Salary)|highest_salary|
    +-------+--------------+-------------------+--------------+
    |Germany|       63000.0|                NaN|         63000|
    | France|       48000.0|                NaN|         48000|
    |  Spain|       62000.0| 12727.922061357855|         71000|
    +-------+--------------+-------------------+--------------+
"""


# -----------------------------------------------------------------------------

zip --recurse-paths pydiaper.zip pydiaper

spark-submit --py-files PY_FILES MAIN_PYTHON_FILE
spark-submit --py-files spark_pipelines/pydiaper/pydiaper.zip ./spark_pipelines/pydiaper/pydiaper/cleaning/clean_ratings.py
