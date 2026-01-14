import sys
import pandas as pd

print('Arguments', sys.argv)
month = int(sys.argv[1])
# print('Hello from pipeline test for month:', month)
print(f'Hello from pipeline test for month:{month}')


df = pd.DataFrame({"month": [1, 2], "num_passengers": [3, 4]})
df["month"] = month
print(df.head())

# df.to_parquet(f"output_day_{sys.argv[1]}.parquet")
df.to_parquet(f'output_{month}.parquet')