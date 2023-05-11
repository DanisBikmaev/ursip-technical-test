import pandas as pd
import datetime as dt
import random
import sqlite3 as sl
import numpy as np

table_headers = ["id", "company", "fact_Qliq_data1", "fact_Qliq_data2", "fact_Qoil_data1", "fact_Qoil_data2",
                 "forecast_Qliq_data1", "forecast_Qliq_data2", "forecast_Qoil_data1", "forecast_Qoil_data2"]


def main():

    data = parse_data("file.xlsx", table_headers)
    table_with_date = add_date_to_table(data)
    write_to_db(table_with_date)
    pivot = make_pivot(table_with_date)
    print(pivot)
    



def parse_data(file_name: str, table_headers: list) -> pd.DataFrame:
    df = pd.read_excel(file_name)
    current_headers = df.head(1).columns.to_list()
    if current_headers != table_headers:
        df = df[2:]
        df.columns = table_headers
        df = df.reset_index(drop=True)
    df = df.drop("id", axis=1)
    return (df)



def generate_random_date():
    start_date = dt.datetime(2023, 5, 1)
    end_date = dt.datetime(2023, 5, 31)
    format = "%d.%m.%Y"
    random_date = start_date + (end_date - start_date) * random.random()
    return random_date.strftime(format)


def add_date_to_table(df: pd.DataFrame):
    df["created_at"] = df.apply(lambda x: str(generate_random_date()), axis=1)
    return df


def write_to_db(df: pd.DataFrame):
    conn = sl.connect("result.sqlite3")
    df.to_sql("parser", conn, if_exists="append", index=False)
    conn.commit()
    conn.close()


def make_pivot(df: pd.DataFrame):
    df_with_total = df.assign(total_fact_Qliq=df['fact_Qliq_data1'] + df['fact_Qliq_data2'],
                            total_fact_Qoil=df['fact_Qoil_data1'] +
                            df['fact_Qoil_data2'],
                            total_forecast_Qliq=df['forecast_Qliq_data1'] +
                            df['forecast_Qliq_data2'],
                            total_forecast_Qoil=df['forecast_Qoil_data1'] + df['forecast_Qoil_data2'])

    df_pivoted_table = pd.pivot_table(df_with_total, index="created_at", values=['fact_Qliq_data1', 'fact_Qliq_data2',
                                                                            'fact_Qoil_data1', 'fact_Qoil_data2',
                                                                            'forecast_Qliq_data1', 'forecast_Qliq_data2',
                                                                            'forecast_Qoil_data1', 'forecast_Qoil_data2',
                                                                            'total_fact_Qliq', 'total_fact_Qoil',
                                                                            'total_forecast_Qliq', 'total_forecast_Qoil'
                                                                            ],
                                    aggfunc=np.sum,
                                    margins=True,
                                    margins_name="total"
                                    )
    return df_pivoted_table

if __name__ == '__main__':
    main()