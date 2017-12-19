"""

Code for ordering the output csv files produced by spendnetwork_record_linkage_example.py by cluster_id.
Helps to understand the results and see how effective the matching was.

"""


import pandas as pd

csv_to_clean = "AC_data_matching_output.csv"
cleaned_output_path = "AC_data_matching_output_cleaned.csv"

df = pd.read_csv(csv_to_clean)
df = df.sort_values(by=["cluster_id", "source_file"])

df.to_csv(cleaned_output_path)
