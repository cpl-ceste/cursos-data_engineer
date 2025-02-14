import os
import json
import awswrangler as wr
import pandas as pd
from datetime import datetime

def lambda_handler(event, context):
    try:
        S3_BUCKET = os.environ["S3_BUCKET"]
        SUMMARY_KEY = os.environ["SUMMARY_KEY"]
        REDSHIFT_CLUSTER = os.environ["REDSHIFT_CLUSTER"]
        REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
        REDSHIFT_USER = os.environ["REDSHIFT_USER"]
        IAM_ROLE = os.environ["IAM_ROLE"]
        REDSHIFT_TABLE = os.environ["REDSHIFT_TABLE"]
        REDSHIFT_SCHEMA = os.environ.get("REDSHIFT_SCHEMA", "public")

        summary_path = f"s3://{S3_BUCKET}/{SUMMARY_KEY}"

        flights_df = wr.s3.read_csv(summary_path)
        flights_df["date_insert"] = pd.to_datetime(datetime.now())

        con = wr.redshift.connect_temp(cluster_identifier=REDSHIFT_CLUSTER,
                                       database=REDSHIFT_DATABASE,
                                       user=REDSHIFT_USER)
        wr.redshift.copy(
            df=flights_df,
            path=f"s3://{S3_BUCKET}/processed/temp",
            con=con,
            schema=REDSHIFT_SCHEMA,
            table=REDSHIFT_TABLE,
            iam_role=IAM_ROLE,
            mode="append"   
        )

        return {"statusCode": 200, "body": json.dumps({"message": "Paso 3 completado"})}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
