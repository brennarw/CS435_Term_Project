import pandas as pd

def generate_table():
    flight_count = pd.read_csv("../Data/bottom50.csv")
    bird_strike_count = pd.read_csv("../Data/bird_strikes/sunset_bird_strike_count_per_airport.csv")
    
    # edit the elements in the airport_code column to remove the first letter
    bird_strike_count["airport_id"] = bird_strike_count["airport_id"].apply(lambda x: x[1:])
    
    print(bird_strike_count.head())
    
    # join the two datasets on airport_id
    joined_df = pd.merge(flight_count, bird_strike_count[["airport_id", "strike_count"]], on="airport_id", how="left")
    
    # add a ratio column
    joined_df["strike_to_flight_ratio"] = joined_df["strike_count"].fillna(0) / joined_df["flight_count"]
    
    # sort based on ratio in ascending order
    sorted_df = joined_df.sort_values(by="strike_to_flight_ratio", ascending=True)
    
    print(joined_df.head())
    
    sorted_df.to_excel("sorted_strike_to_flight_bottom50_ratio.xlsx", index=False)
    

    
if __name__ == "__main__":
    generate_table()
