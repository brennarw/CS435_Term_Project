# import kagglehub

# # Download latest version
# path = kagglehub.dataset_download("robikscube/flight-delay-dataset-20182022")

# print("Path to dataset files:", path)

import re

airport_codes = open("airports-code-world.csv", "r")
flight_dataset = open("C:\\Users\\Spencer\\.cache\\kagglehub\\datasets\\robikscube\\flight-delay-dataset-20182022\\versions\\4\\Combined_Flights_2020.csv", "r")
first_line = True
codes = []
# Extract airport codes from airport-code file
for line in airport_codes:
    if first_line:
        first_line = False
        continue
    
    items = line.split(";")
    # Duplicate check just in case (there were no duplicates)
    if items[0] not in codes:
        codes.append(items[0])

first_line = True
missing_codes = []

for line in flight_dataset:
    if first_line:
        first_line = False
        continue
    line = re.sub(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", ";", line)
    items = line.split(";")
    if items[2] not in codes:
        if items[2] not in missing_codes:
            missing_codes.append(items[2])
        # print("Unknown code found:", items[2])
        # print("   From line:", items)
    if items[3] not in codes:
        if items[3] not in missing_codes:
            missing_codes.append(items[3])
        # print("Unknown code found:", items[3])
        # print("   From line:", items)
    # if items[3] == ' Inc."' or items[2] == ' Inc."':
    #     print(items)
    #     break
    # if len(missing_codes) == 1:
    #     break

print(missing_codes)