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
import csv
    
def create_airport_tiff(csv_path, output_path):
    """
    Create a TIFF file from airport CSV data, filtering for medium and large airports
    in the continental US, and projecting to match the target TIFF CRS.
    """
    # Read CSV file, skipping the second row (metadata)
    # data = pd.read_csv(csv_path, skiprows=[0])
    data = pd.read_csv(csv_path)

    # Convert latitude and longitude to numeric, coercing errors to NaN
    data["longitude_deg"] = pd.to_numeric(data["longitude_deg"], errors='coerce')
    data["latitude_deg"] = pd.to_numeric(data["latitude_deg"], errors='coerce')
    data = data.dropna(subset=["longitude_deg", "latitude_deg"])

    # Swap with us_data below to create tiff with airport locations
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

    # Plot airports into the raster grid with four corners of bounds
    airports_with_coords = []
    j = 0
    for x, y, corners in zip(airport_x, airport_y, valid_corners):
        col = int((x - xmin) / resolution)
        row = int((ymax - y) / resolution)
        for i in range(len(corners)):
            corners[i] = (int((corners[i][0] - xmin) / resolution), int((ymax - corners[i][1]) / resolution))
        if 0 <= row < height and 0 <= col < width:
            raster[row, col] = 1  # Mark airport locations with 1
            airports_with_coords.append((data['airport_id'].iloc[j], col, row, corners))
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
    
    # Return data structure with airport ids, col, row, and corners
    return airports_with_coords

# Given a set of lat and lon values, use GeoPy package to find corners at distance <dist_from_airport>. 
# Returns sets of airport corners transformed for tiff coordinates (row/col)
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

# Overlay fall bird coverage heat map with airports and process bitmapping for region around airport (calculated in create_airport_tiff)
def overlay_two_tiffs(tiff_file_one, airport_tiff, output_file_path, airports_with_coords, labelBool):
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

        # Create visualization
        plt.figure(figsize=(15, 10))

        # Layer 1: First TIFF as background heatmap       
        plt.imshow(norm_data1, cmap="hot", interpolation="nearest", alpha=0.5)
        plt.colorbar(label="Fall Data", fraction=0.046, pad=0.04)

        # Layer 3: Airports with surrounding region
        # Collect corners of airport area and plot (calculated in create_airport_tiff)
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
        
        # Process bit-mapping within specified region around airport
        csv_list = []
        index = 0
        for i in range(0, len(x_coords), 4): # for every four coordinates (each corner bound of the bit-map region)
            csv_line = ""
            x_square = x_coords[i:i + 4] # add 4 x corners
            x_square.append(x_coords[i]) # append duplicate first corner (to create visual square)
            y_square = y_coords[i:i + 4] # add 4 y corners
            y_square.append(y_coords[i]) # append duplicate first corner (to create visual square)
            x_min, x_max = min(x_square), max(x_square)
            y_min, y_max = min(y_square), max(y_square)
            bit_sum = 0
            bit_total = 0
            # Using x-min/max and y-min/max, loop through the entire encompasing rectangle to account for trapozodial regions
            for row in range(y_min, y_max + 1):
                for col in range(x_min, x_max + 1):
                    # if the current pixel is in the trapazoid: all 4 calls to crossProd return the same value
                    # Grab the bit value, add it to the bit_sum, and increment the bit total
                    if crossProd(x_square[2], y_square[2], x_square[3], y_square[3], col, row) ==\
                    crossProd(x_square[3], y_square[3], x_square[0], y_square[0], col, row) ==\
                    crossProd(x_square[0], y_square[0], x_square[1], y_square[1], col, row) ==\
                    crossProd(x_square[1], y_square[1], x_square[2], y_square[2], col, row):
                        bit_sum += norm_data1[row][col]
                        bit_total += 1
            # For each airport, plot all 4 corners (and the duplicate) to create an outline around the airport
            plt.plot(x_square, y_square, color='blue', linewidth=.2)
            # Create line for csv output in the format <airportcode, bit average>
            csv_line = airports_with_coords[index][0] + ',' + str(bit_sum/bit_total)
            csv_list.append(csv_line)
            index += 1
        # Take csv data stored in csv_line and write it to a csv file
        with open("../Data/bitMapping.csv", mode='w', newline='') as file:
            writer = csv.writer(file)
            # Write a header
            writer.writerow(["airport_id", "bird_population"])
            for item in csv_list:
                # Split each string by the comma into name and ratio
                name, ratio = item.split(",")
                # Write the name and ratio as a row in the CSV file
                writer.writerow([name, ratio])
                
        plt.title("Bird Clustering with Airport Locations")
        # If flag was specified to add labels, call addLabel to include airport ids on visualization
        if labelBool :
            addLabel(airports_with_coords, output_file_path)
        # Save the combined visualization without labels
        else:
            plt.savefig(output_file_path, format="png", dpi=1000, bbox_inches="tight")
            plt.close()
            
# Uses the cross product to determine if a given point in the encompasing rectancle is within the target trapezoid
# For all four sides, it the return is the same (all 1 or -1), the point is in the target
def crossProd(x_one, y_one, x_two, y_two, x_test, y_test):
    if ((x_two - x_one) * (y_test - y_one) - (y_two - y_one) * (x_test - x_one)) >= 0:
        return 1
    else: 
        return -1 
    
# Add Airport Ids outside of airport region in visualization
def addLabel(airports_with_coords, output_file_path):
    for airport in airports_with_coords:
        airport_code, airport_x, airport_y, corners = airport
            # Add the label near the airport point, with an offset to place the airport id past the upper right corner of the region
        plt.text(airport_x - 1.0, airport_y - 7.0, airport_code, fontsize=1, ha='left', va='bottom')
    # Save the plot
    plt.savefig(output_file_path, format="png", dpi=2000, bbox_inches="tight")
    plt.close()

# Used for generating overlay of both spring and fall bird coverage heat maps with airport data
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
        
def main(option: str):
    # Hardcoded file paths
# Hardcoded file paths
    # csv_file = "../Data/us-airports.csv"
    # csv_file = "../Data/top50.csv"
    csv_file = "../Data/bird_joined_flight_ratio.csv"
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
