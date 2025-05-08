import requests
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from typing import Dict, Any
import logging
from pyspark.sql.types import StructType, StructField, StringType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the base URLs
base_url_1 = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{}/99495199999.csv"
base_url_2 = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{}/72429793812.csv"

def get_data():
    # Define the range of years
    years = range(2015, 2025)

    # Base directory to save the downloaded files
    base_output_dir = "./weather_data/"

    # Loop through each year and download the CSV files for both datasets
    for year in years:
        # Create a directory for each year
        year_dir = os.path.join(base_output_dir, str(year))
        os.makedirs(year_dir, exist_ok=True)
        
        # Download each file (Florida and Cincinnati)
        for base_url, station_id in [(base_url_1, "99495199999"), (base_url_2, "72429793812")]:
            url = base_url.format(year)
            response = requests.get(url)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Save the file in the appropriate year directory
                file_path = os.path.join(year_dir, f"{station_id}.csv")
                with open(file_path, "wb") as file:
                    file.write(response.content)
                print(f"Downloaded: {file_path}")
            else:
                print(f"Failed to download {url}. Status code: {response.status_code}")


# get_data()


def clean_data():
# Define the base input and output directories
    base_input_dir = "./weather_data/"
    base_output_dir = "./cleaned_weather_data/"

    # Define the invalid value representations
    invalid_values = {
    #     "TEMP": 9999.9,
    #     "DEWP": 9999.9,
    #     "SLP": 9999.9,
    #     "STP": 9999.9,
    #     "VISIB": 999.9,
    #     "WDSP": 999.9,
        "MXSPD": 999.9,
    #     "GUST": 999.9,
        "MAX": 9999.9,
    #     "MIN": 9999.9,
    #     "PRCP": 99.99,
    #     "SNDP": 999.9
    }

    # Loop through each year directory
    for year in range(2015, 2025):
        year_dir = os.path.join(base_input_dir, str(year))
        
        # Check if the year directory exists
        if os.path.exists(year_dir):
            # Loop through each file in the year directory
            for station_id in ["99495199999", "72429793812"]:
                file_path = os.path.join(year_dir, f"{station_id}.csv")
                
                # Check if the file exists
                if os.path.exists(file_path):
                    # Read the CSV file into a DataFrame
                    df = pd.read_csv(file_path)
                    
                    # Filter out rows with invalid values
                    for column, invalid_value in invalid_values.items():
                        df = df[df[column] != invalid_value]
                    
                    # Create the output directory for the year if it doesn't exist
                    output_year_dir = os.path.join(base_output_dir, str(year))
                    os.makedirs(output_year_dir, exist_ok=True)
                    
                    # Save the cleaned DataFrame to the new directory
                    cleaned_file_path = os.path.join(output_year_dir, f"{station_id}.csv")
                    df.to_csv(cleaned_file_path, index=False)
                    print(f"Cleaned data saved to: {cleaned_file_path}")
                else:
                    print(f"File not found: {file_path}")
        else:
            print(f"Year directory not found: {year_dir}")

def create_spark_session(app_name: str = "WeatherDataCount") -> SparkSession:
    """Create and configure a Spark session with optimized settings."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")  # Optimize for small datasets
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.sql.debug.maxToStringFields", "1000")  # Increase field limit
        .getOrCreate()
    )

def get_dataset_counts(spark: SparkSession, base_path: str) -> Dict[str, int]:
    """Count records in each weather dataset."""
    dataset_counts = {}
    
    # Define schema for better performance
    schema = StructType([
        StructField("STATION", StringType(), True),
        StructField("DATE", StringType(), True),
        StructField("NAME", StringType(), True),
        # Add other fields as needed
    ])
    
    for year in range(2015, 2025):
        for station_code in ['99495199999', '72429793812']:
            file_path = os.path.join(base_path, str(year), f"{station_code}.csv")
            if os.path.exists(file_path):
                try:
                    df = spark.read.csv(
                        file_path,
                        header=True,
                        schema=schema,
                        inferSchema=False  # Use predefined schema for better performance
                    )
                    count = df.count()
                    dataset_counts[f"{year}/{station_code}"] = count
                    logger.info(f"Processed {file_path}: {count} records")
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {str(e)}")
            else:
                logger.warning(f"File not found: {file_path}")
    
    return dataset_counts

def main():
    base_path = "./weather_data/"
    
    # Use context manager for SparkSession
    with create_spark_session() as spark:
        try:
            # Get dataset counts
            dataset_counts = get_dataset_counts(spark, base_path)
            
            # Log results
            for dataset, count in dataset_counts.items():
                logger.info(f"Dataset: {dataset}, Count: {count}")
            
            # Initialize a dictionary to store the hottest days per year
            hottest_days = {}
            
            # Loop through the years to find the hottest day
            for year in range(2015, 2025):
                year_dir = os.path.join("./cleaned_weather_data/", str(year))
                for filename in os.listdir(year_dir):
                    if filename.endswith('.csv'):
                        # Read the CSV file into a DataFrame
                        df = spark.read.csv(os.path.join(year_dir, filename), header=True, inferSchema=True)
                        
                        # Check if the DataFrame is empty
                        if df.isEmpty():
                            continue  # Skip to the next file
                        
                        # Check if the "MAX" column exists
                        if "MAX" not in df.columns:
                            print(f"The 'MAX' column does not exist in {filename}.")
                            continue  # Skip to the next file
                        
                        # Find the hottest day for the current DataFrame
                        max_day = df.orderBy(F.desc("MAX")).first()
                        
                        # Check if max_day is None
                        if max_day is not None:
                            # Store the hottest day only if the year is not already recorded
                            if year not in hottest_days:
                                hottest_days[year] = (max_day.STATION, max_day.NAME, max_day.DATE, max_day.MAX)
            
            # Convert results to a DataFrame for display
            if hottest_days:
                hottest_days_list = [(year, *data) for year, data in hottest_days.items()]
                hottest_days_df = spark.createDataFrame(hottest_days_list, ["YEAR", "STATION", "NAME", "DATE", "MAX"])
                hottest_days_df.show()
            else:
                print("No hottest days found across the datasets.")
                
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            raise

if __name__ == "__main__":
    main()