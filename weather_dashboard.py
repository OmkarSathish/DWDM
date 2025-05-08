import streamlit as st
import pandas as pd
import plotly.express as px
import os
import glob
from My_spark import get_data, clean_data
import numpy as np

# Set page config
st.set_page_config(page_title="Weather Data Analysis", layout="wide")

# Title
st.title("🌤️ Weather Data Analysis Dashboard")

# Check if weather_data directory exists
if not os.path.exists("./weather_data"):
    with st.spinner("Downloading weather data... This may take a few minutes."):
        get_data()
        st.success("Data download completed!")
else:
    st.info("Weather data directory already exists. Skipping download.")

# Check if cleaned_weather_data directory exists
if not os.path.exists("./cleaned_weather_data"):
    with st.spinner("Cleaning weather data... This may take a few minutes."):
        clean_data()
        st.success("Data cleaning completed!")
else:
    st.info("Cleaned weather data directory already exists. Skipping cleaning process.")

# Function to load data
@st.cache_data
def load_data():
    data = []
    # Define the invalid value representations
    invalid_values = {
        "MXSPD": 999.9,
        "MAX": 9999.9,
    }
    
    for year in range(2015, 2025):
        year_dir = f"./cleaned_weather_data/{year}"
        if not os.path.exists(year_dir):
            continue  # Skip if year directory doesn't exist
            
        files = glob.glob(f"{year_dir}/*.csv")
        if not files:  # Skip if no files found for this year
            continue
            
        for file in files:
            try:
                df = pd.read_csv(file)
                if df.empty:  # Skip empty dataframes
                    continue
                    
                # Filter out rows with invalid values
                for column, invalid_value in invalid_values.items():
                    if column in df.columns:
                        df = df[df[column] != invalid_value]
                
                df['YEAR'] = year
                # Ensure required columns exist
                required_columns = ['STATION', 'DATE', 'TEMP', 'MAX', 'MIN', 'NAME']
                if all(col in df.columns for col in required_columns):
                    data.append(df)
                else:
                    missing_cols = [col for col in required_columns if col not in df.columns]
                    st.warning(f"Missing columns {missing_cols} in {file} - skipping this file")
            except Exception as e:
                st.warning(f"Could not process {file}: {str(e)} - skipping this file")
                continue
    
    if not data:
        st.error("No valid data found. Please ensure the data files exist in the correct format.")
        return pd.DataFrame()  # Return empty dataframe if no valid data
    
    return pd.concat(data, ignore_index=True)

# Load data with loading state
with st.spinner("Loading and processing data..."):
    df = load_data()

# Check if we have data to display
if df.empty:
    st.error("No data available to display. Please check your data files.")
    st.stop()

# Sidebar controls
st.sidebar.header("Controls")

# Year range slider
year_range = st.sidebar.slider(
    "Select Year Range",
    min_value=2015,
    max_value=2024,
    value=(2015, 2024)
)

# Station selector
stations = df['STATION'].unique()
selected_stations = st.sidebar.multiselect(
    "Select Weather Stations",
    options=stations,
    default=stations
)

# Column selector for analysis
numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
selected_columns = st.sidebar.multiselect(
    "Select Columns to Analyze",
    options=numeric_columns,
    default=['TEMP', 'MAX', 'MIN', 'DEWP', 'WDSP', 'MXSPD', 'GUST', 'PRCP', 'SNDP']
)

# Filter data based on selections
filtered_df = df[
    (df['YEAR'].between(year_range[0], year_range[1])) &
    (df['STATION'].isin(selected_stations))
]

# Main content
st.header("Weather Data Analysis")

# Create tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "Temperature Analysis", 
    "Wind Analysis", 
    "Precipitation Analysis", 
    "Data Summary"
])

with tab1:
    col1, col2 = st.columns(2)
    
    with col1:
        # Temperature over time
        fig_temp = px.line(
            filtered_df,
            x='DATE',
            y='TEMP',
            color='STATION',
            title='Temperature Trends Over Time',
            labels={'TEMP': 'Temperature (°F)', 'DATE': 'Date'}
        )
        st.plotly_chart(fig_temp, use_container_width=True)

        # Temperature distribution
        fig_hist = px.histogram(
            filtered_df,
            x='TEMP',
            color='STATION',
            title='Temperature Distribution',
            labels={'TEMP': 'Temperature (°F)'}
        )
        st.plotly_chart(fig_hist, use_container_width=True)
    
    with col2:
        # Box plot of temperatures
        fig_box = px.box(
            filtered_df,
            x='STATION',
            y='TEMP',
            title='Temperature Distribution by Station',
            labels={'TEMP': 'Temperature (°F)'}
        )
        st.plotly_chart(fig_box, use_container_width=True)

        # Scatter plot of temperature vs dew point
        fig_scatter = px.scatter(
            filtered_df,
            x='TEMP',
            y='DEWP',
            color='STATION',
            title='Temperature vs Dew Point',
            labels={'TEMP': 'Temperature (°F)', 'DEWP': 'Dew Point (°F)'}
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

with tab2:
    col1, col2 = st.columns(2)
    
    with col1:
        # Wind speed over time
        fig_wind = px.line(
            filtered_df,
            x='DATE',
            y='WDSP',
            color='STATION',
            title='Wind Speed Trends',
            labels={'WDSP': 'Wind Speed (mph)', 'DATE': 'Date'}
        )
        st.plotly_chart(fig_wind, use_container_width=True)

        # Maximum wind speed by station
        fig_max_wind = px.bar(
            filtered_df.groupby('STATION')['MXSPD'].max().reset_index(),
            x='STATION',
            y='MXSPD',
            title='Maximum Wind Speed by Station',
            labels={'MXSPD': 'Maximum Wind Speed (mph)'}
        )
        st.plotly_chart(fig_max_wind, use_container_width=True)
    
    with col2:
        # Wind gust distribution
        fig_gust = px.box(
            filtered_df,
            x='STATION',
            y='GUST',
            title='Wind Gust Distribution',
            labels={'GUST': 'Wind Gust (mph)'}
        )
        st.plotly_chart(fig_gust, use_container_width=True)

        # Wind speed vs gust
        fig_wind_gust = px.scatter(
            filtered_df,
            x='WDSP',
            y='GUST',
            color='STATION',
            title='Wind Speed vs Gust',
            labels={'WDSP': 'Wind Speed (mph)', 'GUST': 'Wind Gust (mph)'}
        )
        st.plotly_chart(fig_wind_gust, use_container_width=True)

with tab3:
    col1, col2 = st.columns(2)
    
    with col1:
        # Precipitation over time
        fig_precip = px.line(
            filtered_df,
            x='DATE',
            y='PRCP',
            color='STATION',
            title='Precipitation Over Time',
            labels={'PRCP': 'Precipitation (inches)', 'DATE': 'Date'}
        )
        st.plotly_chart(fig_precip, use_container_width=True)

        # Snow depth distribution
        fig_snow = px.box(
            filtered_df,
            x='STATION',
            y='SNDP',
            title='Snow Depth Distribution',
            labels={'SNDP': 'Snow Depth (inches)'}
        )
        st.plotly_chart(fig_snow, use_container_width=True)
    
    with col2:
        # Precipitation by month
        filtered_df['MONTH'] = pd.to_datetime(filtered_df['DATE']).dt.month
        monthly_precip = filtered_df.groupby(['STATION', 'MONTH'])['PRCP'].sum().reset_index()
        fig_monthly = px.bar(
            monthly_precip,
            x='MONTH',
            y='PRCP',
            color='STATION',
            title='Monthly Precipitation',
            labels={'PRCP': 'Total Precipitation (inches)', 'MONTH': 'Month'}
        )
        st.plotly_chart(fig_monthly, use_container_width=True)

        # Precipitation vs Temperature
        fig_precip_temp = px.scatter(
            filtered_df,
            x='TEMP',
            y='PRCP',
            color='STATION',
            title='Precipitation vs Temperature',
            labels={'TEMP': 'Temperature (°F)', 'PRCP': 'Precipitation (inches)'}
        )
        st.plotly_chart(fig_precip_temp, use_container_width=True)

with tab4:
    # Data summary
    st.subheader("Data Summary")
    
    # Summary statistics for selected columns
    st.write("### Selected Columns Statistics")
    st.dataframe(filtered_df[selected_columns].describe())
    
    # Station information
    st.write("### Station Information")
    station_info = filtered_df.groupby('STATION').agg({
        'NAME': 'first',
        'TEMP': ['mean', 'min', 'max'],
        'PRCP': 'sum',
        'WDSP': 'mean',
        'DATE': 'count'
    }).reset_index()
    station_info.columns = ['Station', 'Location', 'Avg Temp', 'Min Temp', 'Max Temp', 'Total Precipitation', 'Avg Wind Speed', 'Record Count']
    st.dataframe(station_info)

    # Correlation matrix
    st.write("### Correlation Matrix")
    corr_matrix = filtered_df[selected_columns].corr()
    fig_corr = px.imshow(
        corr_matrix,
        title='Correlation Matrix',
        color_continuous_scale='RdBu'
    )
    st.plotly_chart(fig_corr, use_container_width=True)

# Footer
st.markdown("---")
st.markdown("Data source: NOAA Global Summary of the Day") 