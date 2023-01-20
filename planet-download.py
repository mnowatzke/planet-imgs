import os
import errno
import datetime
import time
from zipfile import ZipFile
import json
from pathlib import Path
import tomli
import geopandas as gpd
import shapely
import requests
import rasterio as rio
from requests.auth import HTTPBasicAuth
import matplotlib.pyplot as plt
import numpy as np
import glob

class PlanetConfig:
    def __init__(self, config_file="config.toml"):
        self.config_file = config_file
        # self.order_url = 'https://api.planet.com/compute/ops/orders/v2'
        self.headers = {'content-type': 'application/json'}
        
        try:
            with open(config_file, "rb") as f:
                self.config = tomli.load(f)
        except EnvironmentError as e:
            print(os.strerror(e.errno))
            print("Missing configiguration file.")
            print("Please create a config.toml file with your Planet API key.")
            print("Use config.toml.example as a template.")
        match self.config:
            case {
                "api": {"planet_api_key": str(), 'item_type': str(), 'image_type': str()},
                "filters": {"mask": str(), 'max_cloud': float(), 'start_date': str(), 'end_date': str()},
            }:
                pass
            case ValueError as e:
                print(f'Missing or incorrect value in config.toml')
                print(str(e))
        self.API_KEY = self.config['api']['planet_api_key']
        self.SEARCH_URL = 'https://api.planet.com/data/v1/quick-search'
        self.ITEM_TYPE = self.config['api']['item_type']
        self.IMAGE_TYPE = self.config['api']['image_type']
        self.project_name = self.config['general']['project_name']
        self.auth = HTTPBasicAuth(self.API_KEY, '')
        self.mask = self.config['filters']['mask']
        self.max_cloud = self.config['filters']['max_cloud']
        self.start_date = self.config['filters']['start_date']
        self.end_date = self.config['filters']['end_date']
        
    def __repr__(self) -> str:
        return (
            f"Configuration and filters for Planet API image aquisition"
        )


class PlanetImages:
    def __init__(self):
        self.config = PlanetConfig()
        self.date_folder = f'{self.config.start_date}_{self.config.end_date}'
        self.thumb_dir = os.path.join('data', 'imagery', f'{self.config.project_name}', f'{self.date_folder}', 'thumbnails')
        self.img_dir = os.path.join('data', 'imagery', f'{self.config.project_name}', f'{self.date_folder}')
        self.search_json = {}
        self.img_count = 0
        self.image_ids = []
        self.image_list = []
        self.unique_dates = []
        self.good_imgs = []
        self.imgs_to_download = []
        self.active_imgs = []
        self.all_imgs_active = False
        self.downloaded_imgs = []
    
    def _load_aoi(self):
        aoi_geom = gpd.read_file(self.config.mask)
        aoi_geom = aoi_geom.to_json()
        aoi_geom = json.loads(aoi_geom)
        aoi_coords = aoi_geom['features'][0]['geometry']
        return aoi_coords
    
    def search_for_images(self):
        aoi = self._load_aoi()
        #set start and end dates
        start_date = self.config.start_date + 'T00:00:00.000Z'
        end_date = self.config.end_date + 'T00:00:00.000Z'
        #set the mask or filter for desired AOI
        geometry_filter = {
        "type": "GeometryFilter",
        "field_name": "geometry",
        "config": aoi
        }
        #set range of dates to search for
        date_range_filter = {
        "type": "DateRangeFilter",
        "field_name": "acquired",
        "config": {
            "gte": start_date,
            "lte": end_date
        }
        }

        # filter any images which are more than 10% clouds
        cloud_cover_filter = {
        "type": "RangeFilter",
        "field_name": "cloud_cover",
        "config": {
            "lte": self.config.max_cloud
        }
        }

        # create a filter that combines our geo and date filters
        # could also use an "OrFilter"
        combined_filter = {
        "type": "AndFilter",
        "config": [geometry_filter, date_range_filter, cloud_cover_filter]
        }
        #set what type of Planet scene we want to download
        # API request object
        search_request = {
        "item_types": [self.config.ITEM_TYPE], 
        "filter": combined_filter
        }
        search_result = requests.post(
            self.config.SEARCH_URL,
            auth=HTTPBasicAuth(self.config.API_KEY, ''), json=search_request)
        self.search_json = search_result.json()
        return self.search_json
    
    def get_all_avail_image_ids(self):
        #just return which images are available
        image_json = self.search_for_images()
        self.image_ids = [feature['id'] for feature in image_json['features']]
        print(f'There are {len(self.image_ids)} total images available to download.')
        return self.image_ids
    
    def get_unique_image_dates(self) -> list:
        image_json = self.search_for_images()
        self.image_list = []
        self.unique_dates = []
        #loop through all images, get unique date, and return image info/feature info
        for feature in image_json['features']:
            image_id = feature['id']
            date = image_id[0:8]
            if date not in self.unique_dates:
                self.unique_dates.append(date)
                self.image_list.append(feature)
        num_images = len(self.image_list)
        if num_images != 0:
            print(f'There are {num_images} unique image dates for download.')
            return self.image_list
        else:
            raise ValueError("Image list is empty. Try a new date, location, or filters.")

    def download_image_thumbnails(self) -> None:
        image_list = self.image_list
        img_total = len(image_list)
        counter = 0
        if not os.path.exists(self.thumb_dir):
            os.makedirs(self.thumb_dir)
        for image in image_list:
            thumb_url = image['_links']['thumbnail']
            img_id = image['id']
            thumb_req = requests.get(thumb_url, auth=HTTPBasicAuth(self.config.API_KEY, ''))
            downloaded_thumb = glob.glob(f'{self.thumb_dir}/{img_id}.tif')
            if not downloaded_thumb:
                # print(f"Downloading thumbnail for {img_id}")
                open(f'{self.thumb_dir}/{img_id}.tif', 'wb').write(thumb_req.content)
            else:
                pass
            # print(feature['_links']['thumbnail'])
            # counter += 1
        # return(image_list)

    def filter_images_for_quality(self) -> list:
        thumb_imgs = glob.glob(f'{self.thumb_dir}/*.tif')
        self.good_imgs = []
        for image in thumb_imgs:
            with rio.open(image) as src:
                b, g, r, nir = src.read()
                b_mean = b.mean()
                g_mean = g.mean()
                r_mean = r.mean()
                nir_mean = nir.mean()
                # print(image, b_mean, g_mean, r_mean, nir_mean)
                if nir_mean >= 100 and r_mean < 10:
                    print(f'{image} is possibly bad and/or corrupted. Double check before downloading.')
                else:
                    self.good_imgs.append(image)
        return self.good_imgs

    def get_imgs_to_download(self):
        img_base_names = [os.path.basename(img) for img in self.good_imgs]
        img_base_names = [i[:len(i)-4] for i in img_base_names]
        self.imgs_to_download = [i for i in self.search_json['features'] if i['id'] in img_base_names]
        return self.imgs_to_download

    def activate_imgs(self):
        for i in self.imgs_to_download:
            img_id = i['id']
            img_links = i['_links']
            # dwn_link = img_links['_self']
            asset_url = f'https://api.planet.com/data/v1/item-types/{self.config.ITEM_TYPE}/items/{img_id}/assets'
            try:
                result = \
                requests.get(
                    asset_url,
                    auth=HTTPBasicAuth(self.config.API_KEY, '')
                )
                img_status = result.json()[f'{self.config.IMAGE_TYPE}']['status']
                if img_status == 'inactive':
                    print(f'Image {img_id} is inactive. Attempting to activate.')
                    links = result.json()[f'{self.config.IMAGE_TYPE}']["_links"]
                    # self_link = links["_self"]
                    activation_link = links["activate"]
                    # Request activation of the 'analytic' asset:
                    activate_result = \
                    requests.get(
                        activation_link,
                        auth=HTTPBasicAuth(self.config.API_KEY, '')
                    )
                elif img_status == 'active':
                    print(f'Image {img_id} is already active.')
                else:
                    print(f'Unknown status for image {img_id}')
                    self.imgs_to_download.remove(i)
            except KeyError:
                print(f'Image type {self.config.IMAGE_TYPE} not available for {img_id}')
                print(f'Removing image {img_id} from download list.')
                print(f'These other images are available for {img_id}: {result.json().keys()}')
                self.imgs_to_download.remove(i)
        
    def check_if_images_active(self):
        # check to see if product is active or not
        num_loops = 21
        count = 0
        while(count < num_loops):
            for i in self.imgs_to_download:
                img_id = i['id']
                img_links = i['_links']
                asset_url = img_links['assets']
                # self_link = img_links['_self']
                result = \
                    requests.get(
                        asset_url,
                        auth=HTTPBasicAuth(self.config.API_KEY, '')
                    )
                img_status = result.json()[f'{self.config.IMAGE_TYPE}']['status']   
                success_states = ['active']
                if img_status == 'failed':
                    raise Exception()
                elif img_status in success_states and i not in self.active_imgs:
                    self.active_imgs.append(i)
            count += 1
            if len(self.active_imgs) == len(self.imgs_to_download):
                print('All images ready to download.')
                self.all_imgs_active = True
                break
            time.sleep(30)

    def download_images(self):
        if self.all_imgs_active == True:
            print('Downloading images.')
        else:
            print('All images may not be active. Downloading those that are.')
            print('You may want to run this script again after.')
        ###TODO repeating a lot of requests. Clean this up later and implement other requests above.
        ###TODO mainly there are 2 '_self' links and both are needed. The first is needed to get download link.
        for image in self.active_imgs:
            #go through all active images
            img_id = image['id']
            asset_url = f'https://api.planet.com/data/v1/item-types/{self.config.ITEM_TYPE}/items/{img_id}/assets'
            try:
                #get the asset info
                result = \
                requests.get(
                    asset_url,
                    auth=HTTPBasicAuth(self.config.API_KEY, '')
                )
                #get different _self link for these assets
                links = result.json()[f'{self.config.IMAGE_TYPE}']['_links']
                self_link = links['_self']
                self_req = \
                    requests.get(
                    self_link,
                    auth=HTTPBasicAuth(self.config.API_KEY, '')
                )
                download_url = self_req.json()["location"]
                img_req = requests.get(download_url, auth=HTTPBasicAuth(self.config.API_KEY, ''))
                downloaded_img = glob.glob(f'{self.img_dir}/{img_id}.tif')
                if not downloaded_img:
                    print(f"Downloading image {img_id}.")
                    open(f'{self.img_dir}/{img_id}.tif', 'wb').write(img_req.content)
                else:
                    print(f'Image {img_id} already downloaded -- skipping.')
                    pass
            except:
                print('Something went wrong.')


def main():
    plan_imgs = PlanetImages()
    plan_imgs.get_all_avail_image_ids()
    plan_imgs.get_unique_image_dates()
    plan_imgs.download_image_thumbnails()
    plan_imgs.filter_images_for_quality()
    plan_imgs.get_imgs_to_download()
    plan_imgs.activate_imgs()
    plan_imgs.check_if_images_active()
    plan_imgs.download_images()

if __name__ == "__main__":
    main()
