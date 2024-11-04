import numpy as np
from PIL import Image
import pandas as pd

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
    
    # Convert to numpy array
    array = data.to_numpy().astype(data_type)
    
    # Normalize data to 0-255 range for visualization
    if array.min() != array.max():
        array = ((array - array.min()) * (255.0 / (array.max() - array.min()))).astype('uint8')
    else:
        array = np.zeros_like(array, dtype='uint8')
    
    # Create image from array
    image = Image.fromarray(array)
    
    # Save as TIFF
    image.save(output_path, format='TIFF')

# Example usage
if __name__ == "__main__":
    csv_to_tiff('input.csv', 'output.tif')