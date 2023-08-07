# Loop Take Home Interview

## Problem statement

Loop monitors several restaurants in the US and needs to monitor if the store is online or not. All restaurants are supposed to be online during their business hours. Due to some unknown reasons, a store might go inactive for a few hours. Restaurant owners want to get a report of the how often this happened in the past.

We want to build backend APIs that will help restaurant owners achieve this goal.

We will provide the following data sources which contain all the data that is required to achieve this purpose.

## Data sources

We will have 3 sources of data

- We poll every store roughly every hour and have data about whether the store was active or not in a CSV.  The CSV has 3 columns (`store_id, timestamp_utc, status`) where status is active or inactive.  All timestamps are in **UTC**
  - Data can be found in CSV format [here](https://drive.google.com/file/d/1UIx1hVJ7qt_6oQoGZgb8B3P2vd1FD025/view?usp=sharing)

- We have the business hours of all the stores - schema of this data is `store_id, dayOfWeek(0=Monday, 6=Sunday), start_time_local, end_time_local`
  - These times are in the **local time zone**
  - If data is missing for a store, assume it is open 24*7
  - Data can be found in CSV format [here](https://drive.google.com/file/d/1va1X3ydSh-0Rt1hsy2QSnHRA4w57PcXg/view?usp=sharing)

- Timezone for the stores - schema is `store_id, timezone_str`
  - If data is missing for a store, assume it is America/Chicago
  - This is used so that data sources 1 and 2 can be compared against each other
  - Data can be found in CSV format [here](https://drive.google.com/file/d/101P9quxHoMZMZCVWQ5o-shonk2lgK1-o/view?usp=sharing)

## System requirement

- Do not assume that this data is static and precompute the answers as this data will keep getting updated every hour.
- You need to store these CSVs into a relevant database and make API calls to get the data.

## Data output requirement

We want to output a report to the user that has the following schema

`store_id, uptime_last_hour(in minutes), uptime_last_day(in hours), update_last_week(in hours), downtime_last_hour(in minutes), downtime_last_day(in hours), downtime_last_week(in hours)`

- Uptime and downtime should only include observations within business hours
- You need to extrapolate uptime and downtime based on the periodic polls we have ingested, to the entire time interval
  - eg, business hours for a store are 9 AM to 12 PM on Monday
    - we only have 2 observations for this store on a particular date (Monday) in our data at 10:14 AM and 11:15 AM
    - we need to fill the entire business hours interval with uptime and downtime from these 2 observations based on some sane interpolation logic

Note: The data we have given is a static data set, so you can hard code the current timestamp to be the max timestamp among all the observations in the first CSV.

## API requirement

- You need two APIs
  - /trigger_report endpoint that will trigger report generation from the data provided (stored in DB)
    - No input
    - Output - report_id (random string)
    - report_id will be used for polling the status of report completion
  - /get_report endpoint that will return the status of the report or the csv
    - Input - report_id
    - Output
      - if report generation is not complete, return “Running” as the output
      - if report generation is complete, return “Complete” along with the CSV file with the schema described above.

## Output Video

https://github.com/Kushal-Chandar/loop-take-home-interview/assets/83660514/6b7b0a51-3848-4204-83e2-d6b8ddd05770
