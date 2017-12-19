"""
Cleans up output from gazette script.
Produces 2 cleaned outputs, first ordered by cluster, second showing all the usm3 records first
(so we can easily see how many of them have been matched)

"""

import pandas as pd

csv_to_clean = "gazetteer_output_AC.csv"
cleaned_output_path1 = "gazetteer_output_AC_cleaned1.csv"
cleaned_output_path2 = "gazetteer_output_AC_cleaned2.csv"


df = pd.read_csv(csv_to_clean)

df1 = df.sort_values(by=["cluster_id"] )
df2 = df.sort_values(by=["source_file", "cluster_id"], ascending=(False, True) )


df1.to_csv(cleaned_output_path1)
df2.to_csv(cleaned_output_path2)
