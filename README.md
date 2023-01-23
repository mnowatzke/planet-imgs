# Planet Imgs
Filter, QC, and download Planet satellite images.

You can (and should) create a virtual environment for this script using conda  
(https://www.anaconda.com/products/distribution)
conda create --name planet-imgs --file requirements.txt
This script requires Python >= 3.10

Fill out the config.toml.example file with your project name, desired AOI, date range, and Planet API key.
There is an example mask format in data/masks
Rename the config.toml.example file to config.toml
Run the planet-download.py script and download images for your desired date range