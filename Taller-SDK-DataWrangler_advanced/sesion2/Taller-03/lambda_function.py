import os
import time
import boto3
import pandas as pd
from datetime import datetime
import awswrangler as wr

# Variables de entorno
S3_BUCKET = os.environ["S3_BUCKET"]
SUMMARY_KEY = os.environ["SUMMARY_KEY"]
FEEDBACK_KEY = os.environ["FEEDBACK_KEY"]
REDSHIFT_CLUSTER = os.environ["REDSHIFT_CLUSTER"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_PASSWORD = os.environ["REDSHIFT_PASSWORD"]
REDSHIFT_TABLE = os.environ["REDSHIFT_TABLE"]
REDSHIFT_SCHEMA = os.environ.get("REDSHIFT_SCHEMA", "public")


def lambda_handler(event, context):
    try:
        # Rutas de los archivos
        flight_feedback_summary_lab = f"s3://{S3_BUCKET}/{SUMMARY_KEY}"

        # Leer datos desde S3
        flights_df = wr.s3.read_csv(flight_feedback_summary_lab)
        flights_df["date_insert"] = pd.to_datetime(datetime.now())

        print(flights_df.dtypes)

        con = wr.redshift.connect_temp(
            cluster_identifier=REDSHIFT_CLUSTER, database="dev", user="awsuser"
        )

        wr.redshift.copy(
            df=flights_df,
            path=f"s3://{S3_BUCKET}/processed/temp",
            con=con,
            schema=REDSHIFT_SCHEMA,
            table=REDSHIFT_TABLE,
            iam_role=os.environ["IAM_ROLE"],
            mode="overwrite",
        )

        return {
            "statusCode": 200,
            "body": "Archivos procesados exitosamente",
        }
    except Exception as e:
        return {"statusCode": 500, "body": f"Error durante el procesamiento: {str(e)}"}
