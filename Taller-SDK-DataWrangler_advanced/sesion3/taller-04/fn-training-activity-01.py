import os
import json
import awswrangler as wr
import pandas as pd

def lambda_handler(event, context):
    try:
        S3_BUCKET = os.environ["S3_BUCKET"]
        FLIGHTS_KEY = os.environ["FLIGHTS_KEY"]
        PASSENGERS_KEY = os.environ["PASSENGERS_KEY"]
        REDSHIFT_CLUSTER = os.environ["REDSHIFT_CLUSTER"]
        REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
        REDSHIFT_USER = os.environ["REDSHIFT_USER"]
        IAM_ROLE = os.environ["IAM_ROLE"]
        REDSHIFT_TABLE = os.environ["REDSHIFT_TABLE"]
        REDSHIFT_SCHEMA = os.environ.get("REDSHIFT_SCHEMA", "public")

        flights_path = f"s3://{S3_BUCKET}/{FLIGHTS_KEY}"
        passengers_path = f"s3://{S3_BUCKET}/{PASSENGERS_KEY}"

        flights_df = wr.s3.read_csv(flights_path)
        passengers_df = wr.s3.read_csv(passengers_path)

        merged_df = pd.merge(
            flights_df,
            passengers_df.groupby("flight_number").size().reset_index(name="passenger_count"),
            on="flight_number",
        )
        merged_df["occupancy_rate"] = (
            merged_df["passenger_count"] / merged_df["capacity"]
        ).fillna(0) * 100

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
            mode="append"
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Paso 1 completado", "rows_inserted": len(merged_df)})
        }

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
