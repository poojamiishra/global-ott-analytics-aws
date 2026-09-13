"""AWS Glue ETL job for the Global OTT Content Analytics project.

Expected job arguments:
    --JOB_NAME
    --SOURCE_DATABASE
    --NETFLIX_TABLE
    --TVMAZE_TABLE
    --OUTPUT_PATH

Example OUTPUT_PATH: s3://your-bucket/processed/joined_data/
"""

import sys

import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


ARGUMENT_NAMES = [
    "JOB_NAME",
    "SOURCE_DATABASE",
    "NETFLIX_TABLE",
    "TVMAZE_TABLE",
    "OUTPUT_PATH",
]


def main() -> None:
    args = getResolvedOptions(sys.argv, ARGUMENT_NAMES)

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    netflix_df = glue_context.create_dynamic_frame.from_catalog(
        database=args["SOURCE_DATABASE"],
        table_name=args["NETFLIX_TABLE"],
    ).toDF()

    tvmaze_df = glue_context.create_dynamic_frame.from_catalog(
        database=args["SOURCE_DATABASE"],
        table_name=args["TVMAZE_TABLE"],
    ).toDF()

    netflix_clean = (
        netflix_df.select(
            "show_id",
            "title",
            "type",
            "listed_in",
            "country",
            "release_year",
            "rating",
        )
        .withColumn("title_norm", F.lower(F.trim(F.col("title"))))
        .withColumn("release_year", F.col("release_year").cast("int"))
    )

    tvmaze_clean = (
        tvmaze_df.select(
            "name",
            "type",
            "genres",
            "premiered",
            "language",
            "rating",
            "platform",
            "country",
        )
        .withColumn("title_norm", F.lower(F.trim(F.col("name"))))
        .withColumn("release_year", F.year(F.to_date(F.col("premiered"))))
    )

    joined_df = netflix_clean.alias("netflix").join(
        tvmaze_clean.alias("tvmaze"),
        on=["title_norm", "release_year"],
        how="inner",
    )

    output_df = joined_df.select(
        F.col("netflix.show_id").alias("show_id"),
        F.col("title_norm"),
        F.col("netflix.title").alias("netflix_title"),
        F.col("tvmaze.name").alias("tvmaze_title"),
        F.col("netflix.type").alias("netflix_type"),
        F.col("tvmaze.type").alias("tvmaze_type"),
        F.col("netflix.listed_in").alias("netflix_genres"),
        F.col("tvmaze.genres").alias("tvmaze_genres"),
        F.col("netflix.country").alias("netflix_country"),
        F.col("tvmaze.country").alias("tvmaze_country"),
        F.col("tvmaze.platform").alias("platform"),
        F.col("release_year"),
        F.col("tvmaze.rating").alias("average_rating"),
        F.col("tvmaze.language").alias("language"),
    )

    output_dynamic_frame = DynamicFrame.fromDF(
        output_df,
        glue_context,
        "joined_ott_content",
    )

    glue_context.write_dynamic_frame.from_options(
        frame=output_dynamic_frame,
        connection_type="s3",
        connection_options={"path": args["OUTPUT_PATH"]},
        format="parquet",
    )

    job.commit()


if __name__ == "__main__":
    main()

