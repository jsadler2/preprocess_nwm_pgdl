import pandas as pd
import xarray
import numpy as np
import json
import requests
import datetime

hucs = [f'{h:02}' for h in range(1, 19)]
i = 0
times = []
data = []
for huc in hucs:
    if i<1:
        start_date = "1900-01-01"
        end_date = "2019-09-05"
        date_chunks = pd.date_range(start_date, end_date, periods=20)
        huc_combined = []
        for chunk in date_chunks:
            baseurl = "http://waterservices.usgs.gov/nwis/iv/?format=json&huc={}&startDT={}&endDT={}&parameterCd=00060&siteStatus=all"
            url = baseurl.format(huc, start_date, end_date)
            request_start_time = datetime.datetime.now()
            print(f"starting request for huc {huc} at {request_start_time}")
            r = requests.get(url)
            request_end_time = datetime.datetime.now()
            request_time = request_end_time - request_start_time
            print(f"took {request_time} to get data for huc {huc}")

            data = json.loads(r.text)

            df_collection = []
            time_series = data['value']['timeSeries']
            for ts in time_series:
                site_code = ts['sourceInfo']['siteCode'][0]['value']
                print('processing the data for site ', site_code)
                # this is where the actual data is
                ts_data = ts['values'][0]['value']
                if ts_data:
                    ts_df = pd.DataFrame(ts_data)
                    ts_df.index = pd.to_datetime(ts_df['dateTime'], utc=True)
                    del ts_df['dateTime']
                    del ts_df['qualifiers']
                    # rename the column from 'value' to the site_code
                    ts_df = ts_df.rename(columns={'value': site_code})
                    ts_df[site_code] = pd.to_numeric(ts_df[site_code])
                    ts_df = ts_df.resample('15T').mean()
                    df_collection.append(ts_df)

            i+=1
            chunk_combined_df = pd.concat(df_collection)
            huc_combined.append(combined_df)
        all_huc_df = pd.concat(huc_combined)
        combined_df = combined_df.replace(-999999, np.nan)
        xr = combined_df.to_xarray()
        xr.to_zarr(f'{huc}_streamflow')


