import sqlalchemy
import pandas as pd

engine = sqlalchemy.create_engine('postgresql://root:root@localhost:5432/ny_taxi')
df_iter = pd.read_csv('../../data/yellow_tripdata_2021-01.csv',iterator=True,chunksize=100000)

while True:
    df = next(df_iter)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')