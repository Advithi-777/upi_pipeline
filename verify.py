import pandas as pd
import glob

files = glob.glob("data/bronze/**/*.parquet", recursive=True)
print(f"Files found: {files}")

df = pd.read_parquet(files[0])
print(f"\nShape: {df.shape}")
print(f"\nColumns:\n{df.columns.tolist()}")
print(f"\nSample rows:\n{df.head(3)}")
print(f"\nStatus counts:\n{df['status'].value_counts()}")
print(f"\nAnomalies: {df['is_anomaly'].sum()}")