from pydap.cas.urs import setup_session
import xarray as xr
import time
import s3fs

username = 'jsadler'
password = 'rmX9YgFpxetSoyuxo1Dv'


def pull_nldas_all_at_once(zarr_store):
    base_url = 'https://hydro1.sci.gsfc.nasa.gov/dods/NLDAS_FORA0125_H.002?'
    session = setup_session(username, password, check_url=base_url)

    start_request_time = time.time()
    store = xr.backends.PydapDataStore.open(base_url, session=session)
    ds = xr.open_dataset(store).chunk({'lat': 224, 'lon': 464, 'time': 480})
    ds.to_zarr(zarr_store)
    ds.close()
    end_request_time = time.time()

    print("time elapsed", (end_request_time - start_request_time))


if __name__ == '__main__':
    # set up S3 zarr store
    fs = s3fs.S3FileSystem()
    my_bucket = 'esip-nwm-uswest2/'
    file_name = f'{my_bucket}nwm-dl/nldas'
    s3map = s3fs.S3Map(file_name, s3=fs)
    pull_nldas_all_at_once(s3map)
