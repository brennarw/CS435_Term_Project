#DOWNLOAD matplotlib by running: pip install rasterio matplotlib numpy
import rasterio
import numpy as np
import matplotlib.pyplot as plt

# Open the TIFF file
file_path = "C:/Users/ethan/Downloads/fall_stopover_2500_v9_265_class.tif" #replace witht path to tiff file to be processed
with rasterio.open(file_path) as src:
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
