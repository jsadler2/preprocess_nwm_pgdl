import time
import requests
import json
import ulmo
import hydrofunctions as hf
from hydrofunctions.exceptions import HydroNoDataError


def get_sites_with_param(huc, param):
    # get all the sites in the huc
    sites_in_huc, data = get_sites_in_basin(huc)

    # get all the sites in the huc that have param
    sites_with_param = ulmo.usgs.nwis.get_sites(sites=sites_in_huc, 
                                                parameter_code=param)
    return sites_with_param


