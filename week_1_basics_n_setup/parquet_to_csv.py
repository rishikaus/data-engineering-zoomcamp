# convert ny taxi parquet data to csv

# will need/ want long term solution? 

import pandas as pd

df = pd.read_parquet('/Users/rishi/gitwork/data-engineering-zoomcamp/week_1_basics_n_setup/yellow_tripdata_2021-01.parquet')
df.to_csv('/Users/rishi/gitwork/data-engineering-zoomcamp/week_1_basics_n_setup/yellow_tripdata_2021-01.csv')

### end