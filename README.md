# Weather Data Analysis Dashboard

A comprehensive weather data analysis dashboard that processes and visualizes NOAA Global Summary of the Day data.

## Features
- Real-time weather data visualization
- Temperature, wind, and precipitation analysis
- Interactive data filtering and exploration
- Station-wise weather pattern analysis
- Data summary and correlation analysis

## Setup Instructions

1. Create and activate virtual environment:
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
.\venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the dashboard:
```bash
streamlit run weather_dashboard.py
```

## Project Structure
- `weather_dashboard.py`: Main Streamlit dashboard application
- `My_spark.py`: Data processing and cleaning utilities
- `requirements.txt`: Project dependencies
- `cleaned_weather_data/`: Directory for processed weather data
- `weather_data/`: Directory for raw weather data

## Data Sources
- NOAA Global Summary of the Day
- Weather stations: 99495199999 and 72429793812

## Dashboard Features
- **Temperature Analysis**
  - Temperature trends over time
  - Temperature distribution
  - Temperature by station
  - Temperature vs Dew Point correlation

- **Wind Analysis**
  - Wind speed trends
  - Maximum wind speed by station
  - Wind gust distribution
  - Wind speed vs gust correlation

- **Precipitation Analysis**
  - Precipitation over time
  - Snow depth distribution
  - Monthly precipitation
  - Precipitation vs Temperature correlation

- **Data Summary**
  - Statistical summary of weather metrics
  - Station information
  - Correlation matrix
  - Record counts and averages