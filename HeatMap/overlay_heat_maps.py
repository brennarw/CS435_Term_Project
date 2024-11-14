import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_bounds
from pyproj import Proj, Transformer
import matplotlib.pyplot as plt
import os
import sys

def create_airport_tiff(csv_path, output_path):
    """
    Create a TIFF file from airport CSV data, filtering for medium and large airports
    in the continental US, and projecting to match the target TIFF CRS.
    """
    # Read CSV file, skipping the second row (metadata)
    data = pd.read_csv(csv_path, skiprows=[1])

    # Convert latitude and longitude to numeric, coercing errors to NaN
    data["longitude_deg"] = pd.to_numeric(data["longitude_deg"], errors='coerce')
    data["latitude_deg"] = pd.to_numeric(data["latitude_deg"], errors='coerce')
    data = data.dropna(subset=["longitude_deg", "latitude_deg"])

    # Filter for U.S. medium and large airports
    # us_data = data[
    #     (data["type"].isin(['medium_airport', 'large_airport']))
    # ]
    us_data = data

    # Set up projections
    wgs84 = Proj("EPSG:4326")  # WGS84 (lat/lon)
    target_crs = Proj(proj="aea", lat_1=29.5, lat_2=45.5, lat_0=23, lon_0=-96, x_0=0, y_0=0, datum="WGS84", units="m")
    transformer = Transformer.from_proj(wgs84, target_crs, always_xy=True)

    # Transform coordinates and filter out invalid results
    airport_x, airport_y = transformer.transform(us_data["longitude_deg"].values, us_data["latitude_deg"].values)
    valid_indices = np.isfinite(airport_x) & np.isfinite(airport_y)
    airport_x = airport_x[valid_indices]
    airport_y = airport_y[valid_indices]

    # Set bounds and resolution based on the target TIFF metadata
    xmin, ymin, xmax, ymax = -2356000.0, 272000.0, 2258000.0, 3172000.0
    resolution = 1000  # Based on TIFF metadata
    width = int((xmax - xmin) / resolution)
    height = int((ymax - ymin) / resolution)

    # Create empty raster
    raster = np.zeros((height, width), dtype=np.float32)

    # Plot airports into the raster grid
    for x, y in zip(airport_x, airport_y):
        col = int((x - xmin) / resolution)
        row = int((ymax - y) / resolution)
        if 0 <= row < height and 0 <= col < width:
            raster[row, col] = 1  # Mark airport locations with 1

    # Write to TIFF with matching CRS and transform
    transform = from_bounds(xmin, ymin, xmax, ymax, width, height)
    with rasterio.open(
        output_path,
        'w',
        driver='GTiff',
        height=height,
        width=width,
        count=1,
        dtype='float32',
        crs=target_crs.srs,
        transform=transform
    ) as dst:
        dst.write(raster, 1)

    return output_path

def overlay_three_tiffs(tiff_file_one, tiff_file_two, airport_tiff, output_file_path):
    """
    Overlay three TIFF files: two existing files and the airport data
    """
    # Read all three TIFF files
    with rasterio.open(tiff_file_one) as src1, \
         rasterio.open(tiff_file_two) as src2, \
         rasterio.open(airport_tiff) as src3:

        data1 = src1.read(1)
        data2 = src2.read(1)
        data3 = src3.read(1)

        # Handle NaN values
        data1 = np.nan_to_num(data1, nan=0)
        data2 = np.nan_to_num(data2, nan=0)
        data3 = np.nan_to_num(data3, nan=0)

        # Normalize each dataset
        def normalize(data):
            data_min, data_max = np.min(data), np.max(data)
            if data_max - data_min != 0:
                return (data - data_min) / (data_max - data_min)
            return data

        norm_data1 = normalize(data1)
        norm_data2 = normalize(data2)

        # Get coordinates for airport points
        airport_y, airport_x = np.nonzero(data3)

        # Create visualization
        plt.figure(figsize=(15, 10))

        # Layer 1: First TIFF as background heatmap
        plt.imshow(norm_data1, cmap="hot", interpolation="nearest", alpha=0.5)
        plt.colorbar(label="Fall Data", fraction=0.046, pad=0.04)

        # Layer 2: Second TIFF as scatter overlay
        two_y, two_x = np.nonzero(data2)
        scatter1 = plt.scatter(two_x, two_y, color='#39FF14', s=0.5, alpha=0.6, marker="o")
        plt.colorbar(scatter1, label="Spring Data", fraction=0.046, pad=0.04)

        # Layer 3: Airports as bright points
        scatter2 = plt.scatter(airport_x, airport_y, color='blue', s=20, alpha=1, marker="*")

        plt.title("Combined Visualization with Airport Locations")

        # Save the combined visualization
        plt.savefig(output_file_path, format="png", dpi=300, bbox_inches="tight")
        plt.close()
        
def overlay_two_tiffs(tiff_file_one, airport_tiff, output_file_path):
    """
    Overlay two TIFF files: two existing files and the airport data
    """
    # Read all three TIFF files
    with rasterio.open(tiff_file_one) as src1, \
         rasterio.open(airport_tiff) as src3:

        data1 = src1.read(1)
        data3 = src3.read(1)

        # Handle NaN values
        data1 = np.nan_to_num(data1, nan=0)
        data3 = np.nan_to_num(data3, nan=0)

        # Normalize each dataset
        def normalize(data):
            data_min, data_max = np.min(data), np.max(data)
            if data_max - data_min != 0:
                return (data - data_min) / (data_max - data_min)
            return data

        norm_data1 = normalize(data1)

        # Get coordinates for airport points
        airport_y, airport_x = np.nonzero(data3)

        # Create visualization
        plt.figure(figsize=(15, 10))

        # Layer 1: First TIFF as background heatmap
        plt.imshow(norm_data1, cmap="hot", interpolation="nearest", alpha=0.5)
        plt.colorbar(label="Fall Data", fraction=0.046, pad=0.04)

        # Layer 3: Add a faded halo around the airport points
        # plt.scatter(airport_x, airport_y, color='blue', s=20, alpha=0.2, marker="o")

        # Layer 4: Airports as bright points
        plt.scatter(airport_x, airport_y, color='blue', s=20, alpha=0.5, marker="o")

        plt.title("Combined Visualization with Top 50 Airport Locations")

        # Save the combined visualization
        plt.savefig(output_file_path, format="png", dpi=300, bbox_inches="tight")
        plt.close()

def main(option: str):
    # Hardcoded file paths
    # csv_file = "../Data/us-airports.csv"
    csv_file = "../Data/bird_strikes/sunset_bird_strike_count_per_airport.csv"
    tiff_file_fall = "../Data/fall_stopover_2500_v9_265_class.tif"
    tiff_file_spring = "../Data/spring_stopover_2500_v9_265_class.tif"
    # output_image = f"./heat_maps/{option}_visualization_with_airports.png"
    output_image = "./heat_maps/sunset_birdstrike_visualization.png"
    airport_tiff = "../Data/birdStrike_temp.tif"

    # Create airport TIFF
    create_airport_tiff(csv_file, airport_tiff)
    
    if option == "--Fall":
        overlay_two_tiffs(tiff_file_fall, airport_tiff, output_image)
    elif option == "--Spring":
        overlay_two_tiffs(tiff_file_spring, airport_tiff, output_image)
    elif option == "--Overlay":
        overlay_three_tiffs(tiff_file_fall, tiff_file_spring, airport_tiff, output_image)

    print(f"Heatmap saved to {output_image}")


if __name__ == "__main__":
    # To run the program: python overlay_heat_maps.py [--option]
    option = sys.argv[1]
    main(option)
