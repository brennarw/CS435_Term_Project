#DOWNLOAD matplotlib by running: pip install rasterio matplotlib numpy
import rasterio
import numpy as np
import matplotlib.pyplot as plt
import sys
from rasterio.transform import from_bounds

def tiff_to_heat(tiff_file_path, output_file_path):
    # Open the TIFF file
    # file_path = "C:/Users/ethan/Downloads/fall_stopover_2500_v9_265_class.tif" #replace witht path to tiff file to be processed
    with rasterio.open(tiff_file_path) as src:
        # Read the first band (assuming single-band data)
        data = src.read(1)
        
    # Handle any NaN or extreme values if necessary
    data = np.nan_to_num(data, nan=0)

    # Normalize the data for a smoother heatmap
    data_min, data_max = np.min(data), np.max(data)
    normalized_data = (data - data_min) / (data_max - data_min)

    # Plot heatmap
    plt.figure(figsize=(10, 8))
    plt.imshow(normalized_data, cmap="hot", interpolation="nearest")
    plt.colorbar(label="Intensity")
    plt.title("Heatmap from TIFF Data")
    plt.show()
    plt.savefig(output_file_path, format="png", dpi=300, bbox_inches="tight")
    
    
def tiff_to_scatter_heatmap(tiff_file_path, output_file_path):
    # Open the TIFF file
    # file_path = "C:/Users/ethan/Downloads/fall_stopover_2500_v9_265_class.tif" #replace witht path to tiff file to be processed
    with rasterio.open(tiff_file_path) as src:
        # Read the first band (assuming single-band data)
        data = src.read(1)
        
    # Handle any NaN or extreme values if necessary
    data = np.nan_to_num(data, nan=0)

    # Normalize the data for a smoother heatmap
    data_min, data_max = np.min(data), np.max(data)
    normalized_data = (data - data_min) / (data_max - data_min)

    # generate the x,y coordinates for non-zero points
    y, x = np.nonzero(data) # Returns the indices where data is non-zero
    
    # Plot heatmap
    plt.figure(figsize=(20, 10))
    scatter = plt.scatter(x, y, color='#39FF14', s=0.5, marker="o")
    plt.colorbar(scatter, label="Intensity")
    plt.title("Scatter Heatmap from TIFF Data")
    plt.gca().invert_yaxis() 
    plt.savefig(output_file_path, format="png", dpi=300, bbox_inches="tight", transparent=False)
    plt.close('all')
    
def overlay_tiff_heatmaps(tiff_file_one, tiff_file_two, output_file_path):
    
    xmin, ymin, xmax, ymax = -125, 24, -66, 50  # bounds for the continental U.S.
    
    # Open the TIFF file
    with rasterio.open(tiff_file_one) as src:
        # Get the dimensions of the TIFF file
        width, height = src.width, src.height
        
        # Define the transform based on these bounds and dimensions
        transform = from_bounds(xmin, ymin, xmax, ymax, width, height)
        crs = 'EPSG:4326'  # WGS84 latitude-longitude projection
        
        data1 = src.read(1)
        
    with rasterio.open(tiff_file_two) as src:
        data2 = src.read(1)
        
    # Handle any NaN or extreme values if necessary
    data1 = np.nan_to_num(data1, nan=0)
    data2 = np.nan_to_num(data2, nan=0)

    # Normalize the data for both maps
    data1_min, data1_max = np.min(data1), np.max(data1)
    normalized_data1 = (data1 - data1_min) / (data1_max - data1_min)
    
    data2_min, data2_max = np.min(data2), np.max(data2)
    normalized_data2 = (data2 - data2_min) / (data2_max - data2_min)

    # Generate coordinates for the scatter plot
    one_y, one_x = np.nonzero(data1)
    intensities = normalized_data1[one_y, one_x]

    two_y, two_x = np.nonzero(data2)

    # Start plotting
    plt.figure(figsize=(10, 8))

    # First layer: Heatmap as the background
    plt.imshow(normalized_data1, cmap="hot", interpolation="nearest", alpha=0.5)
    plt.colorbar(label="Heatmap Intensity", fraction=0.046, pad=0.04)

    # Second layer: Scatter plot overlay
    scatter = plt.scatter(two_x, two_y, color='#39FF14', s=0.5, alpha=0.6, marker="o")
    plt.colorbar(scatter, label="Scatter Intensity", fraction=0.046, pad=0.04)

    plt.title("Overlay of Heatmap and Scatter Heatmap from TIFF Data")
    # plt.gca().invert_yaxis()  # Invert y-axis to match image coordinates
    
    # Save the combined overlay plot
    plt.savefig(output_file_path, format="png", dpi=300, bbox_inches="tight")
    plt.show()
    plt.close('all')

if __name__ == "__main__":
    
    if len(sys.argv) < 4:
        print("USAGE: incorrect number of arguments: need a tiff input path (1)")
        sys.exit(0)
    
    if sys.argv[1] == "--Scatter":
        tiff_input_path = sys.argv[2]
        heatmap_output_path = sys.argv[3]
        tiff_to_scatter_heatmap(tiff_input_path, heatmap_output_path)
    if sys.argv[1] == "--HeatMap":
        tiff_input_path = sys.argv[2]
        heatmap_output_path = sys.argv[3]
        tiff_to_heat(tiff_input_path, heatmap_output_path)
    if sys.argv[1] == "--Overlay":
        first_tiff_input_path = sys.argv[2]
        second_tiff_input_path = sys.argv[3]
        heatmap_output_path = sys.argv[4]
        overlay_tiff_heatmaps(first_tiff_input_path, second_tiff_input_path, heatmap_output_path)