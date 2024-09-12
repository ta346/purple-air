import os
import json
import time
import requests
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from io import StringIO
from datetime import datetime
import glob

# Function to update the log file with the last download date and sensor info
def update_sensor_index_log(log_file_path, sensor_list, last_download_date):
    log_data = {}

    # Check if the log file exists, and load existing data if available
    if os.path.exists(log_file_path):
        with open(log_file_path, 'r') as log_file:
            log_data = json.load(log_file)

    # Update the log with the last download date
    log_data["sensors_index.csv file last updated"] = str(last_download_date)
    log_data["sensors_skipped"] = []

    # Add or update each sensor in the log
    for sensor in sensor_list:
        if str(sensor) not in log_data:
            log_data[str(sensor)] = {
                "min_date": None,
                "max_date": None,
                "url_issue": [],
                "no_data": []
            }
    
    # Write the updated log to a JSON file
    with open(log_file_path, 'w') as log_file:
        json.dump(log_data, log_file, indent=4)
    
    print(f"{log_file_path} successfully updated.")
    
# Function to get sensors from the API
def get_sensors(key_read, filename = "sensors_index"):
    
    '''
    Parameters:

    key_read (string): The API key required to access the PurpleAir API.
    filename (string, optional): The name of the output CSV file (default: "sensors_index").
    
    Returns:
    
    The GeoDataFrame containing the sensor data along with the us flag
    
    Detailed Steps:

    1. Define API URL: The function builds a URL to request sensor data from the PurpleAir API, specifying fields like name, location_type, latitude, longitude, and other sensor metadata.
    2. API Call: The function sends a GET request to the API using the constructed URL. If the request is successful (status_code = 200), it loads the JSON response and converts it into a pandas DataFrame. If the request fails, it raises an exception with an error description.
    3. Data Cleaning: Converts UNIX timestamps (last_modified, date_created, last_seen) into a human-readable date format.
    4. Check U.S. Sensors: 
            a. Converts the sensor data into a GeoDataFrame using the latitude and longitude of the sensors.
            b. Loads a GeoJSON file containing U.S. boundaries.
            c. Ensures both datasets share the same coordinate reference system (CRS).
            d. Performs a spatial join to flag sensors that are located in the U.S. (us = 1 if in the U.S., us = 0 otherwise).
    5. Saving Data:
            a. Cleans up unnecessary columns from the GeoDataFrame.
            b. Ensures the target directory for saving the file exists.
            c. Saves the cleaned sensor data as a CSV file.
    6. Logging:
            a. Extracts the list of sensor indices.
            b. Updates a log file (sensor_log.json) with the sensor list and the current date.
    '''
    
    # PurpleAir API URL
    root_url = 'https://api.purpleair.com/v1/sensors/'
    
    # fields to get
    fields_list = ['name', 'location_type', 'latitude', 'longitude', 'altitude', 
                   'position_rating', 'uptime', 'last_seen', 'last_modified', 'date_created']
    
    # Build the fields parameter for the API call
    fields_api_url = '&fields=' + '%2C'.join(fields_list)
    
    # Final API URL
    api_url = root_url + f'?api_key={key_read}' + fields_api_url
    
    print(api_url)
    
    # Getting data
    response = requests.get(api_url)
    if response.status_code == 200:
        json_data = json.loads(response.content)
        df = pd.DataFrame.from_records(json_data["data"])
        df.columns = json_data["fields"]
    else:
        json_data = json.loads(response.content)
        print("Error description:", json_data["description"])
        raise requests.exceptions.RequestException("Failed to fetch sensor data.")
    
    # ----------------------- clean sensor index df
    # Convert UNIX timestamps to readable date format
    df['last_modified'] = pd.to_datetime(df['last_modified'], unit='s')
    df['date_created'] = pd.to_datetime(df['date_created'], unit='s')
    df['last_seen'] = pd.to_datetime(df['last_seen'], unit='s')
    
    # ----------------------- create new column 'us': us = 1 if a sensor is in US; otherwise = 0
    # Create a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs=4326)

    # Load U.S. boundaries from a GeoJSON file
    us_shp = gpd.read_file(os.path.join(data_dir, "us_shp.json"))

    # Ensure the CRS matches
    us_shp.to_crs(gdf.crs, inplace=True)

    # Perform a spatial join to check if the points are in the U.S.
    gdf_us = gpd.sjoin(gdf, us_shp, how="left", predicate="intersects")

    # Add 'us' flag: if the point is in the U.S.
    gdf_us['us'] = gdf_us['index_right'].notnull().astype(int)

    # Drop unnecessary columns
    gdf_sensors = gdf_us.drop(columns=['index_right', 'AFFGEOID', 'GEOID', 'NAME'])

    # Directory to store the file
    os.makedirs(processed_dir, exist_ok=True)  # Create directory if it doesn't exist
    
    filename = os.path.join(processed_dir, filename + ".csv")
    
    print(filename)
    # Define file path and save the CSV file
    gdf_sensors.to_csv(filename, index=False, header=True)

    print(f"{len(gdf_sensors)} sensors successfully extracted and stored in: {filename}")

    # extract sensor_list
    sensor_list = gdf_sensors['sensor_index'].tolist()
    
    # Update the download log
    today = datetime.now().date()

    log_file_path = 'sensor_log.json'
    update_sensor_index_log(log_file_path, sensor_list, today)
    
    return gdf_sensors

def get_sensorslist(filename = "sensors_index"):
    """Retrieve sensor indexes based on whether they are US or not."""
    
    if os.path.exists(os.path.join(processed_dir, filename + '.csv')):
        sensors_index = pd.read_csv(os.path.join(processed_dir, filename + '.csv'))
    else:
        # call get_sensors to new lists of sensor
        sensors_index = get_sensors(key_read = key_read, filename=filename)
        
    # replace this with get_sensor function later!
    # all_sensorlist = gdf_sensorlist['sensor_index'].tolist()
        
    # subset us_indoor and us_oudoor sensors
    # us_sensorlist = sensors_index[sensors_index['us'] == 1]['sensor_index'].tolist()
    us_indoor_sensorlist = sensors_index[(sensors_index['us'] == 1) & (sensors_index['location_type'] == 1)]['sensor_index'].tolist()
    us_outdoor_sensorlist = sensors_index[(sensors_index['us'] == 1) & (sensors_index['location_type'] == 0)]['sensor_index'].tolist()
    
    # non_us_indoor and non_us_outdoor sensors
    nonus_sensorlist = sensors_index[sensors_index['us'] == 0]['sensor_index'].tolist()

    sensor_dict = {
        "us_indoor": us_indoor_sensorlist, 
        "us_outdoor": us_outdoor_sensorlist,
        "non_us": nonus_sensorlist
        }
    
    return sensor_dict

def calculate_pa_points(n_sensors, begin_time, end_time):
    
    # point each request
    rows = 714
    columns = 8 + 2
    point_per_request = int(rows*columns)
    
    # find number of days from begin_time: 01-01-2021 to end_time: 12:31:2023
    # Convert strings to datetime objects
    begin_date = datetime.strptime(begin_time, '%m-%d-%Y')
    end_date = datetime.strptime(end_time, '%m-%d-%Y')

    # Calculate the difference in days
    n_days = (end_date - begin_date).days

    total_points_per_sensor = n_days/5*point_per_request
    
    total_points_n_sensors = int(total_points_per_sensor) * int(n_sensors)
    
    return int(total_points_n_sensors)

def create_pa_datelist(average_time, bdate, edate):
    """Create a list of date ranges for the historical data period."""
    
    # Dates of Historical Data period
    begindate = datetime.fromisoformat(bdate)
    enddate = datetime.fromisoformat(edate)
    
    # Download days based on average
    if average_time == 60:
        datelist = pd.date_range(begindate, enddate, freq='14d')  # 14 days of data
    else:
        datelist = pd.date_range(begindate, enddate, freq='5d')   # 5 days of data

    # Reverse to get data from end date to start date
    datelist = datelist.tolist()
    datelist.reverse()
    
    # Convert to required format
    date_list = [dt.strftime('%Y-%m-%dT%H:%M:%SZ') for dt in datelist]
    
    return date_list

def get_historicaldata(sensors_list, 
                       bdate, 
                       edate, 
                       average_time, 
                       data_dir, 
                       processed_dir, 
                       key_read, 
                       sleep_seconds):
    """
    Purpose:

    This function downloads historical sensor data from the PurpleAir API for a given list of sensors over a specified date range. It processes the data, stores it in separate files per sensor, and updates a log to track downloaded data.

    Parameters:

    sensors_list (list): List of sensor indices to download data for.
    bdate (string): The beginning date (in ISO format) for the data to be retrieved.
    edate (string): The ending date (in ISO format) for the data to be retrieved.
    average_time (int): The average time (in minutes) used for the sensor data.
    data_dir (string): Directory where indoor sensor data is stored.
    processed_dir (string): Directory to save the downloaded sensor data.
    key_read (string): API key to access PurpleAir API.
    sleep_seconds (int): Time in seconds to wait between consecutive API requests to avoid throttling.
    
    Returns:
    
    Save Data: The processed data for each sensor is saved in the folder with sensor ID, using a filename that includes the sensor ID and the date range.
    Log Updates: After processing each sensor, the log file is updated with information about skipped sensors, missing data, and any errors encountered.

    """
    
    # Load the download log file
    log_file_path = 'sensor_log.json'
    with open(log_file_path, 'r') as log_file:
        log_data = json.load(log_file)
    
    # Historical API URL: for multiple sensors
    root_api_url = 'https://api.purpleair.com/v1/sensors/'
    
    # Average time parameter for API
    average_api = f'&average={average_time}'
    
    # Create the fields API URL
    fields_list = ['pm2.5_atm_a', 'pm2.5_atm_b', 'pm2.5_cf_1_a', 'pm2.5_cf_1_b', 'humidity', 'temperature']
    fields_api_url = '&fields=' + '%2C'.join(fields_list)
    
    print("CHECK POINT 1: Fields API URL")
    print(fields_api_url)
    
    # Retrieve sensor index for us_indoor
    us_indoor_sensors = pd.read_csv(os.path.join(data_dir, "indoor_us_jay.csv"))
    us_indoor_sensorlist_jay = us_indoor_sensors['sensor_index'].tolist()
    
    # Generate date list for all sensors
    date_list = create_pa_datelist(average_time, bdate, edate)
    
    print("CHECK POINT 2: Generated Date List")
    print(date_list)
    
    # Generate date list to skip data for certain us_indoor sensors (from 2021-01-01 to 2023-12-31)
    check_datelist = create_pa_datelist(average_time, '2021-01-01T00:00:00+00:00', '2023-12-31T00:00:00+00:00')
    
    len_datelist = len(date_list) - 1
    
    # Process each sensor
    for sensor in sensors_list:
        
        # create new folder for a sensor
        sensor_folder = os.path.join(processed_dir, f"sensorID_{sensor}")
        os.makedirs(sensor_folder, exist_ok=True)  # Create sensor folder if it doesn't exist
        
        hist_api_url = root_api_url + f'{sensor}/history/csv?api_key={key_read}'
        
        # Check if any file exists in the folder: returns a list of files
        existing_files = glob.glob(f'{sensor_folder}/*.csv')
        
        print("CHECK POINT 3: Existing Files")
        print(existing_files)
        
        # If any file exists, use the latest file
        if existing_files:
            # Read the existing file
            existing_df = pd.read_csv(existing_files[0])
        else:
            print(f"No existing files for sensor {sensor}. Starting new file.")
            existing_df = pd.DataFrame()  # No existing data

        # Create start and end date API URL
        for index, date in enumerate(date_list):
            
            # Throttle API requests
            time.sleep(sleep_seconds)  
            
            if index < len_datelist:
                
                # Skip if sensor is in us_indoor and the date is in 2021-2023
                if sensor in us_indoor_sensorlist_jay and min(check_datelist) <= date <= max(check_datelist):
                    print(str(sensor))
                    # Add sensor to skipped list if not already present
                    if sensor not in log_data["sensors_skipped"]:
                        log_data["sensors_skipped"].append(sensor)
                    print(f"Skipping download for {sensor} from {date_list[index+1]} to {date} (us_indoor sensor already downloaded)")
                    continue

                # Download data for PA
                print(f'Downloading for PA: {sensor} for Dates: {date_list[index+1]} and {date}.')
                dates_api_url = f'&start_timestamp={date_list[index+1]}&end_timestamp={date}'
                
                api_url = hist_api_url + dates_api_url + average_api + fields_api_url
                                    
                try:
                    response = requests.get(api_url)
                    response.raise_for_status()  # Raises an exception for 4xx/5xx responses
                    df = pd.read_csv(StringIO(response.text), sep=",", header=0)
                
                # -------------------------- Note
                # Try break for any API related error 
                # or break when only payment issue with API?
                except requests.exceptions.RequestException as e:
                    try:
                        json_data = json.loads(response.content)
                        if json_data["description"] == "Payment is required to make this api call.":
                            print(json_data["description"])
                            break
                        else: 
                            log_data[str(sensor)]["url_issue"].append(json_data["description"])
                    except:
                        log_data[str(sensor)]["url_issue"].append(f"Request failed: {str(e)}")
                    continue
                
                except pd.errors.EmptyDataError:
                    log_data[str(sensor)]["no_data"].append(f"{date_list[index+1]} to {date}")
                    continue
                
                except Exception as e:
                    print(f"Unexpected error for sensor {sensor}: {e}")
                    continue

                if df.empty:
                    log_data[str(sensor)]["no_data"].append(f"{date_list[index+1]} to {date}")
                    continue
                
                # Process DataFrame
                df['time_stamp'] = pd.to_datetime(df['time_stamp'])
                df = df.drop_duplicates().sort_values(by='time_stamp')

                # Append the new data to the existing DataFrame if it exists
                if not existing_df.empty:
                    df = pd.concat([existing_df, df], ignore_index=True)
                    df['time_stamp'] = pd.to_datetime(df['time_stamp'])
                    df = df.drop_duplicates().sort_values(by='time_stamp')

                # Get the minimum and maximum date from the combined DataFrame
                min_date = df['time_stamp'].min()
                max_date = df['time_stamp'].max()
                
                min_date = min_date.strftime("%Y_%m_%d")
                max_date = max_date.strftime("%Y_%m_%d")
                # date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")  
                # date = date.strftime("%Y_%m_%d")
                
                try: 
                    # ---------------------- Note:
                    # Should we use .parquet? Parquet file path (corrected extension to .parquet)
                    # For now, testing on csv
                    csv_file = f'{sensor_folder}/sensorID_{sensor}_{min_date}_{max_date}.csv'

                    # Save the dataframe
                    df.to_csv(csv_file, index=False)
                    print(f"Attempting to save data to: {csv_file}")

                    # If saving succeeds, remove the old file
                    if existing_files:
                        os.remove(existing_files[0])
                    
                    print(f"Data is saved to: {csv_file}")
                    
                except Exception as e:
                    print(f"Error saving new file: {e}")
                    
    # Save the updated log file at the end of the process
    with open(log_file_path, 'w') as log_file:
        json.dump(log_data, log_file, indent=4)
        

def main():
    # API Keys provided by PurpleAir(c)
    key_read = '6CF9C562-6AEB-11EF-95CB-42010A80000E'

    # Sleep Seconds
    sleep_seconds = 3  # wait sleep_seconds after each query

    # Data download period. Enter Start and end Dates
    bdate = '2021-01-01T00:00:00+00:00'
    edate = '2021-01-12T00:00:00+00:00'

    # Average_time. The desired average in minutes
    average_time = 10  # or 10 or 0 (Current script is set only for real-time, 10, or 60 minutes data)

    data_dir = "data"
    processed_dir = "processed"

    try:
        # Example list of sensors for demonstration
        sample_sensors = [131255, 182]

        # Get historical data for the sample sensors
        result = get_historicaldata(
            sensors_list=sample_sensors,
            bdate=bdate,
            edate=edate,
            average_time=average_time,
            data_dir=data_dir,
            processed_dir=processed_dir,
            key_read=key_read,
            sleep_seconds=sleep_seconds
        )

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
    except pd.errors.EmptyDataError:
        print("Data error: No data returned or file is empty.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
    
