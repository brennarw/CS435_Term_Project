import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Read data from a CSV file
def read_csv(file_path):
    data = pd.read_csv(file_path)
    x = data['bird_population']
    y = data['bird_flight_ratio']
    return x, y

# Function to calculate the R-squared value
def calculate_r_squared(x, y, best_fit_line):
    y_pred = best_fit_line(x)  # Predicted values
    ss_total = np.sum((y - np.mean(y)) ** 2)  # Total sum of squares
    ss_residual = np.sum((y - y_pred) ** 2)  # Residual sum of squares
    r_squared = 1 - (ss_residual / ss_total)  # R-squared formula
    return r_squared

# Function to plot scatterplot and line of best fit
def scatterplot_with_fit(file_path):
    # Read data
    x, y = read_csv(file_path)
    
    # Scatterplot
    plt.scatter(x, y, label='Data Points', color='blue')
    
    # Calculate line of best fit
    coefficients = np.polyfit(x, y, 1)  # Linear fit
    best_fit_line = np.poly1d(coefficients)
    
    # Plot line of best fit
    plt.plot(x, best_fit_line(x), label='Line of Best Fit', color='red')
    
    # Calculate R-squared value
    r_squared = calculate_r_squared(x, y, best_fit_line)
    
    # Display R-squared value on the plot
    plt.text(0.05, 0.95, f"$R^2$: {r_squared:.4f}", transform=plt.gca().transAxes,
             fontsize=10, verticalalignment='top', bbox=dict(boxstyle="round", alpha=0.5, color='white'))
    
    # Customize plot
    plt.title('')
    plt.xlabel('Bird Population')
    plt.ylabel('Bird Strikes per Flight')
    plt.legend()
    plt.grid(True)
    
    # Show plot
    plt.show()

# File path to your CSV
csv_file = 'training_data.csv'

# Call the function to plot
scatterplot_with_fit(csv_file)
