import pandas as pd
from scipy.stats import pearsonr

def PearsonAndP():
    # Load CSV file
    csv_file = "../new_training_data.csv"  
    data = pd.read_csv(csv_file)

    # Specify columns for feature and target
    feature_column = "bird_population" # feature
    target_column = "bird_flight_ratio"    #  target

    # Prepare the data
    x = data[feature_column]
    y = data[target_column]
    
    corr, p_val = pearsonr(x, y)
    print(f"P-Value: {p_val:.20f}")
    print(f"Pearson-Value: {corr:.20f}")
    
if __name__ == "__main__":
    PearsonAndP()