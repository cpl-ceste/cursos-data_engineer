import os
import time
import json
import boto3
import pandas as pd
import awswrangler as wr

def lambda_handler(event, context):
    try:
        S3_BUCKET = os.environ["S3_BUCKET"]
        FLIGHTS_KEY = os.environ["FLIGHTS_KEY"]
        FEEDBACK_KEY = os.environ["FEEDBACK_KEY"]
        REDSHIFT_CLUSTER = os.environ["REDSHIFT_CLUSTER"]
        REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
        REDSHIFT_USER = os.environ["REDSHIFT_USER"]
        IAM_ROLE = os.environ["IAM_ROLE"]
        REDSHIFT_TABLE = os.environ["REDSHIFT_TABLE"]
        REDSHIFT_SCHEMA = os.environ.get("REDSHIFT_SCHEMA", "public")
        S3_BUCKET_TARGET = os.environ["S3_BUCKET_TARGET"]

        flights_path = f"s3://{S3_BUCKET}/{FLIGHTS_KEY}"
        feedback_path = f"s3://{S3_BUCKET}/{FEEDBACK_KEY}"

        flights_df = wr.s3.read_csv(flights_path)
        feedback_df = wr.s3.read_csv(feedback_path)

        # Calcula retraso promedio y rating promedio
        delay_avg = flights_df.groupby("flight_number")["delay_minutes"].mean().reset_index(name="average_delay")
        rating_avg = feedback_df.groupby("flight_number")["rating"].mean().reset_index(name="average_rating")

        merged_df = pd.merge(flights_df, delay_avg, on="flight_number", how="left")
        merged_df = pd.merge(merged_df, rating_avg, on="flight_number", how="left")

        # Insertar en Redshift
        con = wr.redshift.connect_temp(cluster_identifier=REDSHIFT_CLUSTER,
                                       database=REDSHIFT_DATABASE,
                                       user=REDSHIFT_USER)
        wr.redshift.copy(
            df=merged_df,
            path=f"s3://{S3_BUCKET}/processed/temp",
            con=con,
            schema=REDSHIFT_SCHEMA,
            table=REDSHIFT_TABLE,
            iam_role=IAM_ROLE,
            mode="overwrite"
        )

        # Guardar CSV en S3
        wr.s3.to_csv(
            df=merged_df,
            path=f"s3://{S3_BUCKET_TARGET}/staging/flight_feedback_summary_lab.csv",
            index=False
        )

        return {"statusCode": 200, "body": json.dumps({"message": "Paso 2 completado"})}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
