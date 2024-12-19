import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

## Parámetros: [JOB_NAME, S3_FLIGHTS_PATH, S3_PASSENGERS_PATH, REDSHIFT_CLUSTER, REDSHIFT_USER, REDSHIFT_DATABASE]
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_FLIGHTS_PATH",
        "S3_PASSENGERS_PATH",
        "REDSHIFT_CLUSTER",
        "REDSHIFT_USER",
        "REDSHIFT_DATABASE",
        "REDSHIFT_TMP_DIR",
        "REDSHIFT_TABLE",
    ],
)

# Configurar Spark y Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Definir rutas S3
flights_path = args["S3_FLIGHTS_PATH"]
passengers_path = args["S3_PASSENGERS_PATH"]


flights_df = spark.read.csv(flights_path, header=True, inferSchema=True)
passengers_df = spark.read.csv(passengers_path, header=True, inferSchema=True)

print("Flights Data:")
flights_df.show()

print("Passengers Data:")
passengers_df.show()

passenger_count_df = passengers_df.groupBy("flight_number").agg(
    F.count("*").alias("passenger_count")
)
merged_df = flights_df.join(passenger_count_df, on="flight_number", how="left")

# Calcular el occupancy_rate
merged_df = merged_df.withColumn(
    "occupancy_rate", F.round((F.col("passenger_count") / F.col("capacity")) * 100, 2)
).fillna(0)

print("Merged DataFrame with Occupancy Rate:")
merged_df.show()

redshift_tmp_dir = args["REDSHIFT_TMP_DIR"]
redshift_table = args["REDSHIFT_TABLE"]

merged_df.write.format("jdbc").option(
    "url",
    f"jdbc:redshift://{args['REDSHIFT_CLUSTER']}:5439/{args['REDSHIFT_DATABASE']}",
).option("dbtable", redshift_table).option("user", args["REDSHIFT_USER"]).option(
    "password", args["REDSHIFT_PASSWORD"]
).option("tempdir", redshift_tmp_dir).mode("overwrite").save()

job.commit()
