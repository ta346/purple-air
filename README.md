# Purple Air

A Python package for downloading and processing sensor data from the PurpleAir API.

## Main Functions

1. **`get_sensors()`**  
   Downloads all available sensors from the PurpleAir API.

2. **`get_sensorlist()`**  
   Wraps `get_sensors()` and creates a dictionary of lists of sensors: `us_indoor`, `us_outdoor`, and `non_us`.

3. **`get_historicaldata()`**  
   Downloads historical sensor data from the PurpleAir API for a given list of sensors over a specified date range. It processes the data, stores it in separate files per sensor, and updates a log to track downloaded data.
   
   **Parameters:**
   - `sensors_list (list)`: List of sensor indices to download data for.
   - `bdate (string)`: The beginning date (in ISO format) for the data to be retrieved.
   - `edate (string)`: The ending date (in ISO format) for the data to be retrieved.
   - `average_time (int)`: The average time (in minutes) for the sensor data (e.g., 0, 10, or 60).
   - `key_read (string)`: API key to access PurpleAir API.
   - `sleep_seconds (int)`: Time in seconds to wait between consecutive API requests to avoid throttling.

## General Workflow

1. Create a virtual Python Conda environment using `environment.yml`. The Conda package will install a new environment called `pair`.
2. Activate the Conda environment:
   ```bash
   conda activate pair
   ```
3. Modify parameters in the environment as needed:
   - `sensors_list (list)` = All available sensors by default.
   - `bdate (string)` = `2021-01-01`.
   - `edate (string)` = `2023-12-31`.
   - `average_time (int)` = `0`, `10`, or `60` (default: 10 minutes).
   - `key_read (string)` = Your PurpleAir API key.
   - `sleep_seconds (int)` = 3 seconds by default.
4. Go to purple-air directory and run the script:
    ```bash
    python purple_air.py
    ``` 

## Result

The script will generate the following files:

1. **Data Folder**  
   Input files that do not change over time.

2. **Processed Folder**
   - `sensors_index.csv`: A list of all available sensors with unique IDs and corresponding attributes.
   - Folders for each sensor:  
     Each folder is named as `sensorID_[sensor_index]`, containing files named as `sensorID_[sensor_index]_[start_date]_[end_date]`.

3. **Log File**
   - `sensor_log.json`: A log file that records error messages and other relevant information during the download process. The structure includes:
     ```json
     {
       "sensors_index.csv file last updated": [],
       "sensors_skipped": [],
       "sensor_index": {
         "min_date": null,
         "max_date": null,
         "url_issue": [],
         "no_data": []
       }
     }
     ```

## Notes

- Make sure your PurpleAir API key is valid to avoid request issues.
- Adjust the `sleep_seconds` parameter to avoid throttling by the PurpleAir API.

## If you have any questions, contact me: atkhusel@stanford.edu

