import numpy as np
from PIL import Image
import pandas as pd
import sys
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import Point
import geopandas as gpd
import matplotlib.pyplot as plt

def csv_to_tiff(csv_path, output_path, data_type='float32'):
    """
    Convert a CSV file to a TIFF image.
    
    Parameters:
    csv_path (str): Path to input CSV file
    output_path (str): Path for output TIFF file
    data_type (str): NumPy data type for the image (default: 'float32')
    
    Returns:
    None
    """
    # Read CSV file
    data = pd.read_csv(csv_path)
    print(data.head())
     # Filter for U.S. longitude and latitude ranges
    us_data = data[
        (data["longitude_deg"] >= -125) & (data["longitude_deg"] <= -66) &
        (data["latitude_deg"] >= 24) & (data["latitude_deg"] <= 50)
    ] # comment this out if you want a tiff file that includes the entire world, not just the US
    
    longitude_values = us_data["longitude_deg"].values
    latitude_values = us_data["latitude_deg"].values
    point_data = us_data["flightCount"] # just holds the airport id
    
    # create a grid extent
    min_lon, max_lon = longitude_values.min(), longitude_values.max()
    min_lat, max_lat = latitude_values.min(), latitude_values.max()
    
    # create a grid
    resolution = 0.01
    width = int((max_lon - min_lon) / resolution)
    height = int((max_lat - min_lat) / resolution)
    
    # create a 2D array (raster) and fill it with values
    raster = np.zeros((height, width), dtype=np.float32)
    
    for lon, lat, value in zip(longitude_values, latitude_values, point_data):
        # convert lat/lon to row/column indices
        row = int((max_lat - lat) / resolution) - 1
        col = int((lon - min_lon) / resolution) - 1
        raster[row, col] = value # assign value to the raster cell
        
    # write to TIFF
    transform = from_origin(min_lon, max_lat, resolution, resolution)
    with rasterio.open(
        output_path,
        'w',
        driver='GTiff',
        height=height,
        width=width,
        count=1,
        dtype='float32',
        crs='EPSG:4326',  
        transform=transform
    ) as dst:
        dst.write(raster, 1)        
    

if __name__ == "__main__":
    
    if len(sys.argv) < 3:
        print("USAGE: incorrect number of arguments: need a csv input path (1) and a tiff output path (2)")
        sys.exit(0)
    csv_path = sys.argv[1]
    tiff_output_path = sys.argv[2]
    # heatmap_output_path = sys.argv[3]
    csv_to_tiff(csv_path, tiff_output_path)