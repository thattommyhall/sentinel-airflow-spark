from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    BinaryType,
    StringType,
    StructField,
    StructType,
    BooleanType,
    ArrayType,
    IntegerType,
)
from pyspark.sql.functions import col, udf, from_json, to_json, lit
from helpers import get_dt, get_start_end_epocs
import pendulum
import pprint
import os

pp = pprint.pprint

spark = SparkSession.builder.enableHiveSupport().appName("parquet-export").getOrCreate()

import yaml

with open("schema.yaml") as f:
    schema = yaml.safe_load(f)

conversions = {
    "bigint": LongType,
    "boolean": BooleanType,
    "numeric": LongType,
    "double precision": LongType,
}


def get_spark_df(table_name):
    start_epoc, end_epoc = get_start_end_epocs()
    df = (
        spark.read.options(header="True", inferSchema="False", multiLine="True")
        .csv(
            f"s3a://sentinel-backfill/raw/{start_epoc}__{end_epoc}/{table_name}.csv",
            quote='"',
            escape='"',
        )
        .dropDuplicates(schema[table_name]["pkeys"])
    )
    column_specs = schema[table_name]["columns"]
    for column_name, spec in column_specs.items():
        data_type = spec["data_type"]
        if data_type not in list(conversions.keys()) + ["text"]:
            raise RuntimeError("could not find data_type to convert: " + data_type)
        if data_type in conversions:
            target_type = conversions[data_type]
            df = df.withColumn(column_name, col(column_name).cast(target_type()))
    if "height" in column_specs:
        df = df.filter(f"{start_epoc} <= height and height < {end_epoc}")
    return df


def export_parquet(table_name):
    dt = get_dt()
    df = get_spark_df(table_name)
    df.write.format("parquet").save(
        f"s3a://sentinel-backfill/athena/{table_name}/dt={dt}", mode="overwrite"
    )


table_name = os.environ["SENTINEL_AIRFLOW_TABLENAME"]
export_parquet(table_name)
