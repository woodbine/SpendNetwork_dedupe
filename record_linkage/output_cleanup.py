import pandas as pd

csv_to_clean = "AC_data_matching_output.csv"
cleaned_output_path = "AC_data_matching_output_cleaned.csv"

df = pd.read_csv(csv_to_clean)
df = df.sort_values(by=["cluster_id", "source_file"])

df.to_csv(cleaned_output_path)
