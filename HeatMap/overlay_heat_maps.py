import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_bounds
from pyproj import Proj, Transformer
from geopy.distance import distance
import matplotlib.pyplot as plt
import os
import sys
import math
    
def create_airport_tiff(csv_path, output_path):
    """
    Create a TIFF file from airport CSV data, filtering for medium and large airports
    in the continental US, and projecting to match the target TIFF CRS.
    """
    # Read CSV file, skipping the second row (metadata)
    # data = pd.read_csv(csv_path, skiprows=[1])
    data = pd.read_csv(csv_path)

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
    lon_inverse, lat_inverse = transformer.transform(airport_x, airport_y, direction='inverse')
    # Calculate 4 corners surrounding airport at given distance
    valid_corners = get_coord_corners(lat_inverse, lon_inverse, transformer)

    # Set bounds and resolution based on the target TIFF metadata
    xmin, ymin, xmax, ymax = -2356000.0, 272000.0, 2258000.0, 3172000.0
    resolution = 1000  # Based on TIFF metadata
    width = int((xmax - xmin) / resolution)
    height = int((ymax - ymin) / resolution)

    # Create empty raster
    raster = np.zeros((height, width), dtype=np.float32)
    airports_with_coords = []

    # Plot airports into the raster grid and get four corners of bounds
    j = 0
    for x, y, corners in zip(airport_x, airport_y, valid_corners):
        col = int((x - xmin) / resolution)
        row = int((ymax - y) / resolution)
        for i in range(len(corners)):
            corners[i] = (int((corners[i][0] - xmin) / resolution), int((ymax - corners[i][1]) / resolution))
        if 0 <= row < height and 0 <= col < width:
            raster[row, col] = 1  # Mark airport locations with 1
            airports_with_coords.append((data['airportId'].iloc[j], col, row, corners))
        j += 1
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
        
    return airports_with_coords

def get_coord_corners(lat_set, lon_set, transformer):
    directions = [45, 135, 225, 315]
    all_squares = []
    dist_from_airport = 5 # Distance from airport to corners, add <* math.sqrt(2)> to make this the distance from the airport to the central bound
    for lat, lon in zip(lat_set, lon_set):
        square = []
        for direction in directions:
            new_point = distance(miles=dist_from_airport).destination((lat, lon), direction)
            square.append(transformer.transform( new_point.longitude, new_point.latitude))
        all_squares.append(square)
    return all_squares

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
        
def overlay_two_tiffs(tiff_file_one, airport_tiff, output_file_path, airports_with_coords, legendBool):
    """
    Overlay two TIFF files: two existing files and the airport data
    """
    # Read all three TIFF files
    with rasterio.open(tiff_file_one) as src1, \
         rasterio.open(airport_tiff) as src3:

        data1 = src1.read(1)
        data3 = src3.read(1)
        transform = src3.transform 

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
        # Generate size of mile radius for plot point
        # Where 1000 represents the tiff resolution
        # Layer 4: Airports as bright points
        # Collect corners of airport area and plot
        x_coords = []
        y_coords = []
        for port in airports_with_coords:
            x_coords.append(port[3][0][0])
            x_coords.append(port[3][1][0])
            x_coords.append(port[3][2][0])
            x_coords.append(port[3][3][0])
            y_coords.append(port[3][0][1])
            y_coords.append(port[3][1][1])
            y_coords.append(port[3][2][1])
            y_coords.append(port[3][3][1])
            
        for i in range(0, len(x_coords), 4):
            x_square = x_coords[i:i + 4]
            x_square.append(x_coords[i])
            y_square = y_coords[i:i +4]
            y_square.append(y_coords[i])
            plt.plot(x_square, y_square, color='blue', linewidth=.2)

        plt.title("Combined Visualization with Top 50 Airport Locations")
        if legendBool :
            addLegend(airports_with_coords, output_file_path)
        # Save the combined visualization
        else:
            plt.savefig(output_file_path, format="png", dpi=1000, bbox_inches="tight")
            plt.close()
            
def addLegend(airports_with_coords, output_file_path):
    # Read the CSV file
    # Create the plot for every 4 points so that airports are not connected to one another
    for airport in airports_with_coords:
        airport_code, airport_x, airport_y, corners = airport

            # Add the label near the airport point, with an offset to center the airport name
        plt.text(airport_x - 1.0, airport_y - 7.0, airport_code, fontsize=1, ha='left', va='bottom')
    # Save the plot
    plt.savefig(output_file_path, format="png", dpi=1000, bbox_inches="tight")
    plt.close()
    
def main(option: str):
    # Hardcoded file paths
# Hardcoded file paths
    # csv_file = "../Data/us-airports.csv"
    csv_file = "../Data/top50.csv"
    tiff_file_fall = "../Data/fall_stopover_2500_v9_265_class.tif"
    tiff_file_spring = "../Data/spring_stopover_2500_v9_265_class.tif"
    # output_image = f"./heat_maps/{option}_visualization_with_airports.png"
    output_image = "./heat_maps/sunset_birdstrike_visualization.png"
    airport_tiff = "../Data/birdStrike_temp.tif"    

    # Create airport TIFF
    airports_with_coords = create_airport_tiff(csv_file, airport_tiff)
    
    if option == "--Fall":
        overlay_two_tiffs(tiff_file_fall, airport_tiff, output_image, airports_with_coords, False)
    elif option == "--FallLabel":
        overlay_two_tiffs(tiff_file_fall, airport_tiff, output_image, airports_with_coords, True)
    elif option == "--Spring":
        overlay_two_tiffs(tiff_file_spring, airport_tiff, output_image)
    elif option == "--Overlay":
        overlay_three_tiffs(tiff_file_fall, tiff_file_spring, airport_tiff, output_image)
    print(f"Heatmap saved to {output_image}")

if __name__ == "__main__":
    # To run the program: python overlay_heat_maps.py [--option]
    option = sys.argv[1]
    main(option)
