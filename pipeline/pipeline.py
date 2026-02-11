
import sys

import pandas as pd

print("arguments", sys.argv)

month = int(sys.argv[1])

df = pd.DataFrame({"day": [1,2], "num_passenger": [3, 4]})
df['month'] = month
print(df.head()) #returns first 5 rows by default

df.to_parquet(f"output_{month}.parquet")

print(f"Hello pipeline, month = {month}")



