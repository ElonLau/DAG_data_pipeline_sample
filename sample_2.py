
# Testing your data pipeline
# unit testing

from datetime import date
from pyspark.sql import Row

Record = Row("country", "utm_campaign", "airtime_in_minutes", "start_date", "end_date")

# Create a tuple of records
data = (
  Record("USA", "DiapersFirst", 28, date(2017, 1, 20), date(2017, 1, 27)),
  Record("Germany", "WindelKind", 31, date(2017, 1, 25), None),
  Record("India", "CloseToCloth", 32, date(2017, 1, 25), date(2017, 2, 2))
)

# Create a DataFrame from these records
frame = spark.createDataFrame(data)
frame.show()

"""
<script.py> output:
    +-------+------------+------------------+----------+----------+
    |country|utm_campaign|airtime_in_minutes|start_date|  end_date|
    +-------+------------+------------------+----------+----------+
    |    USA|DiapersFirst|                28|2017-01-20|2017-01-27|
    |Germany|  WindelKind|                31|2017-01-25|      null|
    |  India|CloseToCloth|                32|2017-01-25|2017-02-02|
    +-------+------------+------------------+----------+----------+
"""


# ---------------------------------------------------------------------------------


# test_improvements.py

    spark = SparkSession.builder.getOrCreate()

    fields = [
        StructField("country", StringType(), True),
        StructField("province", StringType(), True),
        StructField("inhabitants", LongType(), True),
        StructField("foo", BooleanType(), True),  # distinctive features
    ]

    frame = spark.createDataFrame({
        ("China", "A", 3, False),
        ("China", "A", 2, True),
        ("China", "B", 14, False),
        ("US", "A", 4, False)},
        schema=StructType(fields)
    )
    actual = aggregate_inhabitants_by_province(frame).cache()

    # In the older implementation, the data was first filtered for a specific
    # country, after which you'd aggregate by province. The same province
    # name could occur in multiple countries though.
    # This test is expecting the data to be grouped by country,
    # then province from aggregate_inhabitants_by_province()
    expected = spark.createDataFrame(
        {("China", "A", 5), ("China", "B", 14), ("US", "A", 4)},
        schema=StructType(fields[:3])
    ).cache()

    # testing using assert statement
    assert actual.schema == expected.schema, "schemas don't match up"
    assert sorted(actual.collect()) == sorted(expected.collect()),\
        "data isn't equal"




# chinese_provinces_improved.py

def store_chinese_demographics(frame, catalog):
    frame.write.parquet(catalog["business/chinese_demographics"])


# Improved aggregation function, grouped by country and province
def aggregate_inhabitants_by_province(frame):
    return (frame
            .groupBy("country", "province")         # first by country and then by province
            .agg(sum(col("inhabitants")).alias("inhabitants"))
            )


def main():
    spark = SparkSession.builder.getOrCreate()
    frame = extract_demographics(spark, catalog)
    chinese_demographics = frame.filter(lower(col("country")) == "china")
    aggregated_demographics = aggregate_inhabitants_by_province(chinese_demographics)
    store_chinese_demographics(aggregated_demographics, catalog)


if __name__ == "__main__":
    main()


# ---------------------------------------------------------------------------------
# Managing and orchestrating a workflow

from datetime import datetime
from airflow import DAG

reporting_dag = DAG(
    dag_id="publish_EMEA_sales_report",
    # Insert the cron expression
    schedule_interval="0 7 * * 1",
    start_date=datetime(2019, 11, 24),
    default_args={"owner": "sales"}
)


# Specify direction using verbose method
prepare_crust.set_downstream(apply_tomato_sauce)

tasks_with_tomato_sauce_parent = [add_cheese, add_ham, add_olives, add_mushroom]
for task in tasks_with_tomato_sauce_parent:
    # Specify direction using verbose method on relevant task
    apply_tomato_sauce.set_downstream(task)

# Specify direction using bitshift operator
tasks_with_tomato_sauce_parent >> bake_pizza

# Specify direction using verbose method
bake_pizza.set_upstream(prepare_oven)


# ------------------------------------
# Create a DAG object
dag = DAG(
  dag_id='optimize_diaper_purchases',
  default_args={
    # Don't email on failure
    'email_on_failure': False,
    # Specify when tasks should have started earliest
    'start_date': datetime(2019, 6, 25)
  },
  # Run the DAG daily
  schedule_interval='@daily')


  config = os.path.join(os.environ["AIRFLOW_HOME"],
                      "scripts",
                      "configs",
                      "data_lake.conf")

ingest = BashOperator(
  # Assign a descriptive id
  task_id="ingest_data",
  # Complete the ingestion pipeline
  bash_command='tap-marketing-api | target-csv --config %s' % config,
  dag=dag)


  # Import the operator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

# Set the path for our files.
entry_point = os.path.join(os.environ["AIRFLOW_HOME"], "scripts", "clean_ratings.py")
dependency_path = os.path.join(os.environ["AIRFLOW_HOME"], "dependencies", "pydiaper.zip")

with DAG('data_pipeline', start_date=datetime(2019, 6, 25),
         schedule_interval='@daily') as dag:
  	# Define task clean, running a cleaning job.
    clean_data = SparkSubmitOperator(
        application=entry_point,
        py_files=dependency_path,
        task_id='clean_data',
        conn_id='spark_default')

# -------------------------------------------------------------------------

spark_args = {"py_files": dependency_path,
              "conn_id": "spark_default"}
# Define ingest, clean and transform job.
with dag:
    ingest = BashOperator(task_id='Ingest_data', bash_command='tap-marketing-api | target-csv --config %s' % config)
    clean = SparkSubmitOperator(application=clean_path, task_id='clean_data', **spark_args)
    insight = SparkSubmitOperator(application=transform_path, task_id='show_report', **spark_args)

    # set triggering sequence
    ingest >> clean >> insight

# -------------------------------------------------------------------------------

"""
An Apache Airflow DAG used in course material.

"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    #    "owner": "squad-a",
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 5),
    "email": ["foo@bar.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "cleaning",
    default_args=default_args,
    user_defined_macros={"env": Variable.get("environment")},
    schedule_interval="0 5 */2 * *"
)


def say(what):
    print(what)


with dag:
    say_hello = BashOperator(task_id="say-hello", bash_command="echo Hello,")
    say_world = BashOperator(task_id="say-world", bash_command="echo World")
    shout = PythonOperator(task_id="shout",
                           python_callable=say,
                           op_kwargs={'what': '!'})

    say_hello >> say_world >> shout


#--------------------------------------------------

    """
An Apache Airflow DAG used in course material.

"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "squad-a",
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 5),
    "email": ["foo@bar.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "cleaning",
    default_args=default_args,
    user_defined_macros={"env": Variable.get("environment")},
    schedule_interval="0 5 */2 * *"
)


def say(what):
    print(what)


with dag:
    say_hello = BashOperator(task_id="say-hello", bash_command="echo Hello,")
    say_world = BashOperator(task_id="say-world", bash_command="echo World")
    shout = PythonOperator(task_id="shout",
                           python_callable=say,
                           op_kwargs={'what': '!'})

    say_hello >> say_world >> shout
