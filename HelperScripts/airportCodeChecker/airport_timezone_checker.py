import re

airport_codes = open("airports-code-world.csv", "r")
timezones_dataset = open("timezones.csv", "r")
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

for line in timezones_dataset:
    if first_line:
        first_line = False
        continue
    line = re.sub(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", ";", line)
    items = line.split(";")
    code = items[0].strip("\"")
    if code not in codes:
        if code not in missing_codes:
            missing_codes.append(code)
        # print("Unknown code found:", items[2])
        # print("   From line:", items)
        # print("Unknown code found:", items[3])
        # print("   From line:", items)
    # if items[3] == ' Inc."' or items[2] == ' Inc."':
    #     print(items)
    #     break
    # if len(missing_codes) == 1:
    #     break

print(missing_codes)