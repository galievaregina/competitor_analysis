import pandas as pd
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import datetime
from collections import defaultdict


def get_diff_price(start, end, competitor):
    start1 = (datetime.datetime.strptime(start, '%Y-%m-%d') - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    cnt_days = (datetime.datetime.strptime(end, '%Y-%m-%d') - (datetime.datetime.strptime(start1, '%Y-%m-%d'))).days + 1
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
        cursor.execute(
            fr"SELECT id_config, price,date FROM price WHERE date BETWEEN '{start1}' AND '{end}' AND id_config IN (SELECT id_config FROM configurations WHERE provider = '{competitor}') ORDER BY date ASC")
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['id_config', 'price', 'date'])
        group_id = df.groupby('id_config')
        price_change = defaultdict(list)
        for id, id_data in group_id:
            if len(id_data) == cnt_days:
                i = 0
                while i < cnt_days:
                    if i != 0:
                        price_change[id].append(
                            round(id_data.iloc[[i - 1]]['price'].iloc[0] / id_data.iloc[[i]]['price'].iloc[0], 1))
                    i += 1
        res = pd.DataFrame(price_change)
        res = res.transpose()
        return pd.DataFrame(data={'date': pd.Series(pd.date_range(datetime.datetime.strptime(start, '%Y-%m-%d'),
                                                                  datetime.datetime.strptime(end, '%Y-%m-%d')).strftime(
            '%Y-%m-%d')).tolist(), 'diff': res.mean()})

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


if __name__ == '__main__':
    print(get_diff_price('2022-12-05', '2022-12-05', 'reg_ru'))
