import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import text
import missingno as msno
import matplotlib.pyplot as plt

engine = create_engine('postgresql://postgres:Qurbanov4868@localhost:5432/us_air_pollution_db', echo=True)

# data.to_sql('temp-table', engine, if_exists="replace")

# with engine.connect() as conn:
#     conn.execute(text("ALTER TABLE \"air-pollution\" DROP COLUMN \"Unnamed: 0\";"))
#     conn.commit()

query = text("""
SELECT *
FROM "air-pollution"
WHERE "Address" = '5888 MISSION BLVD., RUBIDOUX'
ORDER BY "Date Local" ASC
""")

# with engine.connect() as conn:
#     columns = conn.execute(query).fetchall()
#
#     for column in columns:
#         column_name, data_type = column
#
#         if data_type == 'bigint':
#             conn.execute(text(f"ALTER TABLE \"air-pollution\" ALTER COLUMN \"{column_name}\" TYPE int;"))
#             conn.commit()

# with engine.begin() as conn:
#     conn.execute(query)

query_second = """
SELECT "Date Local", "NO2 AQI", "O3 AQI", "SO2 AQI", "CO AQI"
FROM "air-pollution"
WHERE "Address" = '5888 MISSION BLVD., RUBIDOUX'
ORDER BY "Date Local" ASC, "index" ASC;
"""

# with engine.connect() as conn, conn.begin():
#     data = pd.read_sql(query, conn)
#     print(data.info())
#     ax = msno.bar(data, fontsize=12)
#     ax.set_title('Missing Data Bar Plot', fontsize=20)
#     plt.subplots_adjust(bottom=0.2)
#     plt.show()


def first_non_nan(series):
    return series.dropna().iloc[0] if not series.dropna().empty else np.nan


def last_non_nan(series):
    return series.dropna().tail(1) if not series.dropna().empty else np.nan


with engine.connect() as conn, conn.begin():
    data = pd.read_sql(query_second, conn)
    plt.figure(figsize=(15, 7))
    data['Date Local'] = pd.to_datetime(data['Date Local'])

    start_date = "2000-01-01"
    end_date = "2016-01-01"

    mask = (data['Date Local'] >= start_date) & (data['Date Local'] <= end_date)
    data_filtered = data[mask]

    print(data_filtered.info())

    # first_non_nan_data = data_filtered.groupby("Date Local").agg({"SO2 AQI": first_non_nan}).reset_index()
    last_non_nan_data_NO2 = data_filtered.groupby("Date Local").agg({"NO2 AQI": last_non_nan}).reset_index()
    last_non_nan_data_O3 = data_filtered.groupby("Date Local").agg({"O3 AQI": last_non_nan}).reset_index()
    last_non_nan_data_SO2 = data_filtered.groupby("Date Local").agg({"SO2 AQI": last_non_nan}).reset_index()
    last_non_nan_data_CO = data_filtered.groupby("Date Local").agg({"CO AQI": last_non_nan}).reset_index()

    print(last_non_nan_data_NO2.info())

    full_date_range = pd.date_range(start=start_date, end=end_date)

    last_non_nan_data_NO2.set_index('Date Local', inplace=True)
    last_non_nan_data_O3.set_index('Date Local', inplace=True)
    last_non_nan_data_SO2.set_index('Date Local', inplace=True)
    last_non_nan_data_CO.set_index('Date Local', inplace=True)

    last_non_nan_data_NO2 = last_non_nan_data_NO2.reindex(full_date_range)
    last_non_nan_data_O3 = last_non_nan_data_O3.reindex(full_date_range)
    last_non_nan_data_SO2 = last_non_nan_data_SO2.reindex(full_date_range)
    last_non_nan_data_CO = last_non_nan_data_CO.reindex(full_date_range)

    plt.plot(last_non_nan_data_NO2.index, last_non_nan_data_NO2['NO2 AQI'], label='NO2 AQI last non-NaN', color='red')
    plt.plot(last_non_nan_data_O3.index, last_non_nan_data_O3['O3 AQI'], label='O3 AQI last non-NaN', color='blue')
    plt.plot(last_non_nan_data_SO2.index, last_non_nan_data_SO2['SO2 AQI'], label='SO2 AQI last non-NaN', color='green')
    plt.plot(last_non_nan_data_CO.index, last_non_nan_data_CO['CO AQI'], label='CO AQI last non-NaN', color='cyan')

    plt.title('All AQI values 2000-2016')
    plt.xlabel('Date')
    plt.ylabel('AQI Value')
    plt.legend()

    plt.tight_layout()
    plt.show()
