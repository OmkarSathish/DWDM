import requests
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URLS = {
    "72421093814": "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{}/7241093814.csv",
    "99495199999": "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{}/99495199999.csv",
    "72429793812": "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{}/72429793812.csv"
}

WEATHER_SCHEMA = StructType([
    StructField("STATION", StringType(), True),
    StructField("DATE", DateType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("ELEVATION", DoubleType(), True),
    StructField("NAME", StringType(), True),
    StructField("TEMP", DoubleType(), True),
    StructField("DEWP", DoubleType(), True),
    StructField("SLP", DoubleType(), True),
    StructField("STP", DoubleType(), True),
    StructField("VISIB", DoubleType(), True),
    StructField("WDSP", DoubleType(), True),
    StructField("MXSPD", DoubleType(), True),
    StructField("GUST", DoubleType(), True),
    StructField("MAX", DoubleType(), True),
    StructField("MIN", DoubleType(), True),
    StructField("PRCP", DoubleType(), True),
    StructField("SNDP", DoubleType(), True),
    StructField("FRSHTT", StringType(), True)
])

INVALID_VALUES = {
    "TEMP": 9999.9,
    "DEWP": 9999.9,
    "SLP": 9999.9,
    "STP": 9999.9,
    "VISIB": 999.9,
    "WDSP": 999.9,
    "MXSPD": 999.9,
    "GUST": 999.9,
    "MAX": 9999.9,
    "MIN": 9999.9,
    "PRCP": 99.99,
    "SNDP": 999.9
}

class WeatherDataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.base_input_dir = Path("./weather_data")
        self.base_output_dir = Path("./cleaned_weather_data")
        
    def download_data(self, year: int) -> None:
        year_dir = self.base_input_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        
        for station_id, url_template in BASE_URLS.items():
            url = url_template.format(year)
            try:
                response = requests.get(url)
                response.raise_for_status()
                
                file_path = year_dir / f"{station_id}.csv"
                with open(file_path, "wb") as file:
                    file.write(response.content)
                logger.info(f"Downloaded: {file_path}")
            except requests.RequestException as e:
                logger.error(f"Failed to download {url}: {str(e)}")
                
    def clean_data(self, year: int) -> None:
        year_dir = self.base_input_dir / str(year)
        output_year_dir = self.base_output_dir / str(year)
        output_year_dir.mkdir(parents=True, exist_ok=True)
        
        for station_id in BASE_URLS.keys():
            file_path = year_dir / f"{station_id}.csv"
            if not file_path.exists():
                logger.warning(f"File not found: {file_path}")
                continue
                
            try:
                df = self.spark.read.csv(
                    str(file_path),
                    header=True,
                    schema=WEATHER_SCHEMA,
                    dateFormat="yyyy-MM-dd"
                )
                
                cleaned_df = self._clean_dataframe(df)
                
                output_path = output_year_dir / f"{station_id}.csv"
                cleaned_df.write.csv(
                    str(output_path),
                    header=True,
                    mode="overwrite"
                )
                logger.info(f"Cleaned data saved to: {output_path}")
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {str(e)}")
                
    def _clean_dataframe(self, df) -> Any:
        for column, invalid_value in INVALID_VALUES.items():
            if column in df.columns:
                df = df.filter(F.col(column) != invalid_value)
        
        numeric_columns = [field.name for field in WEATHER_SCHEMA.fields 
                         if isinstance(field.dataType, DoubleType)]
        
        for column in numeric_columns:
            if column in df.columns:
                df = df.fillna({column: 0.0})
        
        return df
    
    def analyze_data(self, year: int) -> Dict[str, Any]:
        year_dir = self.base_output_dir / str(year)
        if not year_dir.exists():
            return {}
            
        analysis_results = {}
        
        for station_id in BASE_URLS.keys():
            file_path = year_dir / f"{station_id}.csv"
            if not file_path.exists():
                continue
                
            try:
                df = self.spark.read.csv(
                    str(file_path),
                    header=True,
                    schema=WEATHER_SCHEMA,
                    dateFormat="yyyy-MM-dd"
                )
                
                stats = df.select([
                    F.avg("TEMP").alias("avg_temp"),
                    F.max("TEMP").alias("max_temp"),
                    F.min("TEMP").alias("min_temp"),
                    F.avg("PRCP").alias("avg_precip"),
                    F.sum("PRCP").alias("total_precip"),
                    F.avg("WDSP").alias("avg_wind_speed"),
                    F.max("MXSPD").alias("max_wind_speed")
                ]).collect()[0]
                
                analysis_results[station_id] = {
                    "avg_temp": stats["avg_temp"],
                    "max_temp": stats["max_temp"],
                    "min_temp": stats["min_temp"],
                    "avg_precip": stats["avg_precip"],
                    "total_precip": stats["total_precip"],
                    "avg_wind_speed": stats["avg_wind_speed"],
                    "max_wind_speed": stats["max_wind_speed"]
                }
                
            except Exception as e:
                logger.error(f"Error analyzing {file_path}: {str(e)}")
                
        return analysis_results

def create_spark_session(app_name: str = "WeatherDataProcessor") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

def get_data():
    with create_spark_session() as spark:
        processor = WeatherDataProcessor(spark)
        for year in range(2015, 2025):
            processor.download_data(year)

def clean_data():
    with create_spark_session() as spark:
        processor = WeatherDataProcessor(spark)
        for year in range(2015, 2025):
            processor.clean_data(year)

def analyze_weather_data():
    with create_spark_session() as spark:
        processor = WeatherDataProcessor(spark)
        all_analysis = {}
        for year in range(2015, 2025):
            year_analysis = processor.analyze_data(year)
            if year_analysis:
                all_analysis[year] = year_analysis
        return all_analysis

if __name__ == "__main__":
    analysis_results = analyze_weather_data()
    for year, results in analysis_results.items():
        logger.info(f"\nAnalysis for year {year}:")
        for station, stats in results.items():
            logger.info(f"\nStation {station}:")
            for metric, value in stats.items():
                logger.info(f"{metric}: {value}")