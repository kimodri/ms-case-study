import os, json
import pandas as pd
files = os.listdir('./files')
with open(f"files/{files[-1]}", 'r', encoding="utf-8") as f:
    data = json.load(f)
print(type(data.get('modules'))) # list

print(pd.DataFrame(data.get('modules')).head())
