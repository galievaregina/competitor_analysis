import pandas as pd
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import datetime
import numpy as np
from collections import defaultdict


def get_diff_price(start, end):
    start1 = (datetime.datetime.strptime(start, '%d_%m_%Y') - datetime.timedelta(days=1)).strftime("%d_%m_%Y")

    try:
        # Подключение к существующей базе данных
        connection = psycopg2.connect(database="Competitor_analysis",
                                      user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password="2320uhbR",
                                      host="127.0.0.1",
                                      port="5432")
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        # Курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        cursor.execute(f"SELECT id_server,price,date FROM hostkey WHERE date BETWEEN '{start1}' AND '{end}'")
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['id', 'price', 'date'])
        group_id = df.groupby('id')
        price_change = defaultdict(list)
        for id, id_data in group_id:
            i = 0
            while i < len(id_data):
                if i != 0:
                    price_change[id].append(id_data.iloc[[i - 1]]['price'].iloc[0] / id_data.iloc[[i]]['price'].iloc[0])
                i += 1
        res = pd.DataFrame(price_change)
        res = res.transpose()
        return [pd.date_range(datetime.datetime.strptime(start, '%d_%m_%Y'),
                              datetime.datetime.strptime(end, '%d_%m_%Y')).strftime('%d_%m_%Y').tolist(),
                res.mean().tolist()]

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


if __name__ == '__main__':
    print(get_diff_price('20_11_2022', '22_11_2022'))
