import os
import argparse


from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    StringType,
    DoubleType,
    IntegerType,
)
from pyspark.sql.functions import regexp_replace, lit
from dotenv import load_dotenv

load_dotenv()


def weather_data_cli():
    parser = argparse.ArgumentParser(
        description="Load weather data for a specific weather station"
    )
    parser.add_argument(
        "-p", "--path", type=str, help="weather data folder path", required=True
    )
    parser.add_argument(
        "-st",
        "--station",
        type=str,
        help="meteorological station from which the data was extracted",
        choices=[
            "INAOE",
            "GTM-SIERRA-NEGRA",
            "BUAP-CRC",
            "UPAEP-PUE",
            "ITP",
            "COATZACOALCOS",
        ],
        required=True,
    )
    return parser.parse_args()


def load_weather_data():
    stations = {
        "INAOE": 1,
        "GTM-SIERRA-NEGRA": 2,
        "BUAP-CRC": 3,
        "UPAEP-PUE": 4,
        "ITP": 5,
        "COATZACOALCOS": 6,
    }
    # get cli args
    args = weather_data_cli()

    spark_ctx = SparkSession.builder.appName("loadWeatherData").getOrCreate()

    # create schema
    weather_schema = StructType(
        [
            StructField("Fecha (America/Mexico_City)", DateType(), False),
            StructField("Temp (°C)", StringType(), True),
            StructField("Chill (°C)", StringType(), True),
            StructField("Dew (°C)", StringType(), True),
            StructField("Heat (°C)", StringType(), True),
            StructField("Hum (%)", StringType(), True),
            StructField("Bar (mmHg)", StringType(), True),
            StructField("Rain (mm)", StringType(), True),
        ]
    )

    # load files
    weather_data_df = (
        spark_ctx.read.schema(schema=weather_schema)
        .option("header", "true")
        .csv(
            path=args.path,
            encoding="UTF-8",
            sep=";",
        )
    )

    # rename columns
    new_header = ["record_date", "temp", "chill", "dew", "heat", "hum", "bar", "rain"]
    weather_data_df = weather_data_df.toDF(*new_header)

    # transform data
    # replace null values with "0"
    weather_data_df = weather_data_df.na.fill(value="0")
    # replace "," for "." and convert to double
    weather_data_df = weather_data_df.withColumns(
        {
            "temp": regexp_replace("temp", ",", ".").cast(DoubleType()),
            "chill": regexp_replace("chill", ",", ".").cast(DoubleType()),
            "dew": regexp_replace("dew", ",", ".").cast(DoubleType()),
            "heat": regexp_replace("heat", ",", ".").cast(DoubleType()),
            "hum": regexp_replace("hum", ",", ".").cast(DoubleType()),
            "bar": regexp_replace("bar", ",", ".").cast(DoubleType()),
            "rain": regexp_replace("rain", ",", ".").cast(DoubleType()),
            "station_id": lit(stations[args.station]).cast(IntegerType()),
        },
    )
    # # add the station_id to the DF
    # weather_data_df = weather_data_df.withColumn("station_id", lit(1))

    # display the first ten columns of the transformed data
    weather_data_df.show(n=10)

    # get env vars
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")

    # charge data to the DB
    URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    TABLE = "analysis_weatherrecord"
    MODE = "append"
    PROPERTIES = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver",
    }
    weather_data_df.write.jdbc(url=URL, table=TABLE, mode=MODE, properties=PROPERTIES)

    spark_ctx.stop()


def main():
    load_weather_data()


if __name__ == "__main__":
    main()
