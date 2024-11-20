import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

# Load CSV file
csv_file = "../training_data.csv"  # Replace with your CSV file path
data = pd.read_csv(csv_file)

# Specify columns for feature and target
feature_column = "bird_population"  # Replace with the column name for the feature
target_column = "bird_flight_ratio"    # Replace with the column name for the target
identifier_column = "airport_id"   # Replace with the column name for the identifier (e.g., airport name)

# Prepare the data
X = data[[feature_column]]
y = data[target_column]
identifiers = data[identifier_column]  # Keep the identifier column

# Split into training and testing sets
X_train, X_test, y_train, y_test, identifiers_train, identifiers_test = train_test_split(
    X, y, identifiers, test_size=0.2, random_state=42)

# Create and train the Random Forest model
model = RandomForestRegressor(random_state=42)
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

# Print model statistics
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"Mean Squared Error: {mse:.10f}")
print(f"R-squared: {r2:.2f}")

# Debugging: Print actual vs predicted values
print("\nActual vs. Predicted values:")
print("Actual: ", y_test.values[:10])  # Print the first 10 actual values
print("Predicted: ", y_pred[:10])  # Print the first 10 predicted values

# Create a DataFrame to compare actual vs predicted values with the airport name preserved
comparison = pd.DataFrame({
    "Airport": identifiers_test,  # Add the airport column for identification
    "Actual": y_test,
    "Predicted": y_pred
}).reset_index(drop=True)

print("\nActual vs. Predicted Values with Airport:")
print(comparison)
