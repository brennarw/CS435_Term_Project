import pandas as pd
import random
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from scipy.stats import pearsonr
from scipy.stats import ttest_ind

def BuildAndTestTree(test):
    # Load CSV file
    csv_file = "../new_training_data.csv"  
    data = pd.read_csv(csv_file)

    # Specify columns for feature and target
    feature_columns = ["bird_population"] # feature
    target_column = "bird_flight_ratio"    #  target
    id_column = "airport_id"   # id

    # Prepare the data
    x = data[feature_columns]
    y = data[target_column]
    ids = data[id_column]  # Keep the identifier column

    # Split into training and testing sets
    x_train, x_test, y_train, y_test, ids_train, ids_test = train_test_split(
    x, y, ids, test_size=0.2, random_state=42)

    # Create and train the Random Forest model
    model = RandomForestRegressor(n_estimators=100, max_depth=10)
    model.fit(x_train, y_train)

    # Make predictions
    y_pred = model.predict(x_test)
    # Print model statistics
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    corr, p_val = pearsonr(x[feature_columns].values.flatten(), y)
    #p_val = ttest_ind(x[feature_columns].values.flatten(), y).pvalue
    print(f"P-Value: {p_val:.30f}")
    print(f"Pearson-Value: {corr:.20f}")
    print(f"Mean Squared Error: {mse:.10f}")
    print(f"R-squared: {r2:.2f}")
    # Create a DataFrame to compare actual vs predicted values with the airport name preserved
    comparison = pd.DataFrame({
    "Airport": ids_test,  
    "Actual": y_test,
    "Predicted": y_pred
    }).reset_index(drop=True)

    print("\nActual vs. Predicted Values with Airport:")
    print(comparison)
    test_pred = model.predict(test)
    comparison = pd.DataFrame({
    "Population": test,  
    "Predicted": test_pred
    }).reset_index(drop=True)
    print("\nRandom Populations and Predicted Values:")
    print(comparison)
    
if __name__ == "__main__":
    test_population = []
    sub_val = []
    for i in range(0,20):
        sub_val.append(random.random())
        test_population.append(sub_val)
        sub_val = []
    BuildAndTestTree(test_population)