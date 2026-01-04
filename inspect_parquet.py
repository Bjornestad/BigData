import pyarrow.parquet as pq

file_path = 'problem_file.parquet'
try:
    parquet_file = pq.ParquetFile(file_path)

    print("--- File Metadata ---")
    print(parquet_file.metadata)

    print("\n--- Schema ---")
    print(parquet_file.schema)

    print("\n--- Created By ---")
    print(parquet_file.metadata.created_by)

except Exception as e:
    print(f"Error reading Parquet file: {e}")
