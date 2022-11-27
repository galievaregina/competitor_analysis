# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from datetime import date
import pandas as pd
import json
import requests
import re
from fake_headers import Headers
import time
from bs4 import BeautifulSoup
import datetime
from sqlalchemy import create_engine

current_date = date.today()
currentDay = current_date.day
currentMonth = current_date.month
currentYear = current_date.year

def load_data(url):
    header = Headers(headers=False)
    header = header.generate()
    try:
        servers = requests.get(url, headers=header, verify=False)
    except requests.exceptions.HTTPError as err:
        time.sleep(10)
        raise SystemExit(err)
    return servers


def load_hetzner(url):
    servers_hetzner = load_data(url)
    servers_hetzner = servers_hetzner.content.decode('utf-8')
    servers_hetzner = re.sub('\\n', ' ', servers_hetzner)
    servers_hetzner = json.loads(servers_hetzner)
    servers = servers_hetzner['server']
    print(servers)
    data_hetzner = pd.DataFrame()
    for server in servers:
        key = server['key']
        name = server['name']
        cpu = server['cpu']
        cores = server['cores']
        frequency = server['frequency']
        ram = server['ram']
        if "DDR4" in server['ram_hr']:
            ddr4 = 1
            ddr3 = 0
        else:
            ddr4 = 0
            ddr3 = 1
        # is_ecc = server['isEcc']
        price = server['price'] * 1.19
        setup_price = server['setup_price'] * 1.19
        total_price = price + setup_price
        day = currentDay
        month = currentMonth
        year = currentYear
        nvme = sum(server['serverDiskData']['nvme'])
        sata = sum(server['serverDiskData']['sata'])
        hdd = sum(server['serverDiskData']['hdd'])
        datacenters = server['datacenter']
        dc_str = ''
        for dc in datacenters:
            dc_str = dc_str + dc['name'] + ' ; '
        row = pd.Series(
            [name, key, cpu, cores, frequency, ram, ddr4, ddr3, hdd, sata, nvme, price, setup_price, total_price,
             dc_str, day, month, year],
            index=['name', 'key', 'cpu', 'cores', 'frequency',
                   'ram', 'ddr4', 'ddr3', 'hdd_size', 'ssd_size', 'nvme_size', 'price', 'setup_price', 'total_price',
                   'datacenter', 'day', 'month', 'year'])
        data_hetzner = pd.concat([data_hetzner, row], axis=1)
    data_hetzner = data_hetzner.transpose()
    data_hetzner['cpu'] = data_hetzner['cpu'].str.replace(r'-', ' ')
    data_hetzner['cpu'] = data_hetzner['cpu'].str.replace(r'".*"', ' ')
    data_hetzner['cpu'] = data_hetzner['cpu'].str.replace(r'[®™]', '')
    data_hetzner['cpu'] = data_hetzner['cpu'].str.replace(r'(AMD|Intel)', '')
    data_hetzner['datacenter'] = data_hetzner['datacenter'].str.replace(r' ;', '')
    data_hetzner.to_csv(fr'D:\competitor_analysis\hetzner\{current_date}.csv',
                        index=False)


def load_servers_ru(url):
    servers_ru = load_data(url).json()
    servers_ru = servers_ru['data']
    data_servers_ru = pd.DataFrame()
    counter = 0

    for server in servers_ru:
        id = server['id']
        cpu_name = server['cpu_name']
        cpu_count = server['processor_count']
        gpu_count = server['gpu_count']
        cores = server['cores']
        freq = server['processor_speed'] / 1000
        ram = server['ram_size']
        if "DDR4" == server['ram_type']:
            ddr4 = 1
            ddr3 = 0
        else:
            ddr4 = 0
            ddr3 = 1
        disks = server['hdds_description']
        datacenter = server['location_name'].split(' ')[0]
        price = server['prices']['full']['hosting']['total']
        day = currentDay
        month = currentMonth
        year = currentYear

        config_row = [id, cpu_name, cpu_count, gpu_count, cores, freq, ram, ddr4, ddr3, disks, datacenter, price, day, month, year]
        config_row = pd.Series(config_row, index=['id', 'cpu_name', 'cpu_count', 'gpu_count', 'cores', 'frequency',
                                                  'ram', 'ddr4', 'ddr3', 'disks', 'datacenter', 'price', 'day', 'month', 'year'],
                               name=counter)

        data_servers_ru = pd.concat([data_servers_ru, config_row], axis=1, sort=False)
        counter += 1

    data_servers_ru = data_servers_ru.transpose()

    # data_servers_ru = data_servers_ru.loc[data_servers_ru['loc_id'].isin([5,6,1,20])]

    data_servers_ru['cpu_name'] = data_servers_ru['cpu_name'].str.replace(r'Intel ', '')
    data_servers_ru['disks'] = data_servers_ru['disks'].str.lower()
    data_servers_ru['disks'] = data_servers_ru['disks'].str.replace(r'\"', '')
    data_servers_ru['disks'] = data_servers_ru['disks'].str.replace(r'sata', 'hdd')
    data_servers_ru['disks'] = data_servers_ru['disks'].str.replace(r'sas', 'hdd')

    def unpack_disks(disks):
        output = {
            'hdd': 0,
            'ssd': 0,
            'nvme': 0
        }
        disks = disks.split(',')
        for disk in disks:
            disk = re.findall(r'(\d+ x \d+|\d+) (gb|tb) (ssd|hdd|nvme)', disk)[0]
            if disk[1] == 'tb':
                size = re.findall(r'(\d+)', disk[0])
                if len(size) == 1:
                    output[disk[2]] = output[disk[2]] + int(size[0]) * 1000
                else:
                    output[disk[2]] = output[disk[2]] + int(size[0]) * int(size[1]) * 1000
            else:
                size = re.findall(r'(\d+)', disk[0])
                if len(size) == 1:
                    output[disk[2]] = output[disk[2]] + int(size[0])
                else:
                    output[disk[2]] = output[disk[2]] + int(size[0]) * int(size[1])
        return output['hdd'], output['ssd'], output['nvme']

    data_servers_ru['hdd_size'], data_servers_ru['ssd_size'], data_servers_ru['nvme_size'] = zip(
        *data_servers_ru['disks'].apply(unpack_disks))
    data_servers_ru = data_servers_ru[
        ['id', 'cpu_name', 'cpu_count', 'gpu_count', 'cores', 'frequency', 'ram', 'ddr4', 'ddr3',
         'hdd_size', 'ssd_size', 'nvme_size', 'datacenter', 'price', 'day', 'month', 'year']]
    data_servers_ru = data_servers_ru.astype(
        {'id': int, 'cpu_name': str, 'cpu_count': int, 'gpu_count': str, 'cores': int, 'frequency': float, 'ram': int,
         'ddr4': int, 'ddr3': int, 'hdd_size': int, 'ssd_size': int, 'nvme_size': int, 'price': float,
         'datacenter': str, 'day':int, 'month': int, 'year': int})
    engine = create_engine('postgresql://postgres:2320uhbR@127.0.0.1:5432/Competitor_analysis')
    data_servers_ru.to_sql('servers_ru', engine, if_exists='append', index=False)


def load_hostkey(url):
    def unpack_disks_hostkey(disks):
        output = {
            'hdd': 0,
            'ssd': 0,
            'nvme': 0
        }
        disks = disks.split(' ')
        size = disks[0].replace('gb', '')
        if disks[1] == 'hdd':
            output[disks[1]] = size
        elif disks[1] == 'ssd':
            output[disks[1]] = size
        else:
            output[disks[1]] = size
        return output['hdd'], output['ssd'], output['nvme']

    def create_df(url):
        hostkey_servers = load_data(url).json()
        hostkey_servers = hostkey_servers['response']
        data_hostkey_servers = pd.DataFrame()
        counter = 0
        for server in hostkey_servers:
            name = server['common']['name']
            location = server['common']['location']
            current_price = server['common']['conditions']['items'][0]['prices']['current']
            limit_order = server['common']['conditions']['limit_order']
            a = server['hardware']['cpu']['description']
            a = a.replace('xx*', ' ')
            # a = a.replace('X', '')
            a = a.split('x')
            if len(a) == 1:
                cpu_count = 1
                cpu_name = a[0]
            else:
                cpu_count = a[0]
                cpu_name = a[1]
            cores = server['hardware']['cpu']['number_cores']
            freq = server['hardware']['cpu']['items'][0]['ghz']
            ram = server['hardware']['ram']['volume']
            disks = server['hardware']['hard_drive']['description'].lower()
            id_data = str(server['common']['id'])
            id = id_data + location
            day = currentDay
            month = currentMonth
            year = currentYear
            config_row = [id, name, cpu_name, cpu_count, cores, freq, ram, disks, location, current_price, limit_order,
                          day, month, year]

            config_row = pd.Series(config_row,
                                   index=['id', 'name', 'cpu_name', 'cpu_count', 'cores', 'frequency', 'ram', 'disks',
                                          'datacenter', 'price', ' limit_order', 'day', 'month', 'year'], name=counter)

            data_hostkey_servers = pd.concat([data_hostkey_servers, config_row], axis=1, sort=False)

            counter += 1

        data_hostkey_servers = data_hostkey_servers.transpose()
        data_hostkey_servers['hdd_size'], data_hostkey_servers['ssd_size'], data_hostkey_servers['nvme_size'] = zip(
            *data_hostkey_servers['disks'].apply(unpack_disks_hostkey))
        data_hostkey_servers = data_hostkey_servers[
            ['id', 'name', 'cpu_name', 'cpu_count', 'cores', 'frequency', 'ram', 'hdd_size', 'ssd_size',
             'nvme_size', 'datacenter', 'price', ' limit_order', 'day', 'month', 'year']]
        data_hostkey_servers = data_hostkey_servers.astype(
            {'id': str, 'name': str, 'cpu_name': str, 'cpu_count': int, 'cores': int,
             'frequency': float, 'ram': int, 'hdd_size': int, 'ssd_size': int,
             'nvme_size': int, 'price': float, 'datacenter': str, 'day': int, 'month': int, 'year': int})
        return data_hostkey_servers

    d_NL = create_df(url[0])
    d_USA = create_df(url[1])
    d_R = create_df(url[2])
    data = [d_NL, d_USA, d_R]
    res_hostkey = pd.concat(data)
    engine = create_engine('postgresql://postgres:2320uhbR@127.0.0.1:5432/Competitor_analysis')
    res_hostkey.to_sql('hostkey', engine, if_exists='append', index=False)


def load_timeweb(url):
    timeweb_servers = load_data(url).json()
    timeweb_servers = timeweb_servers['body']
    data_timeweb_servers = pd.DataFrame()
    counter = 0
    for server in timeweb_servers:
        id = server['config_plan_id']
        a = server['cpu_vendor_short'].split('x')
        if len(a) == 1:
            cpu_count = 1
            cpu_name = a[0]
        else:
            cpu_count = a[0]
            cpu_name = a[1]
        cores = server['cpu_cores']
        freq = server['cpu_vendor'].split(',')[1].split('-')[0]
        ram = int(server['memory']) // 1000
        if "DDR4" == server['memory_type']:
            ddr4 = 1
            ddr3 = 0
        else:
            ddr4 = 0
            ddr3 = 1
        disks = server['disk_desc'].lower()
        if disks.__contains__('geforce'):
            parts = disks.split('+')
            gpu = parts[1]
            disks = parts[0]
        else:
            gpu = 0
        price = server['price']
        day = currentDay
        month = currentMonth
        year = currentYear
        config_row = [id, cpu_name, cpu_count, cores, freq, ram, ddr4, ddr3, disks, gpu, price, day, month, year]
        config_row = pd.Series(config_row, index=['id', 'cpu_name', 'cpu_count', 'cores', 'frequency',
                                                  'ram', 'ddr4', 'ddr3', 'disks', 'gpu', 'price', 'day', 'month', 'year'], name=counter)

        data_timeweb_servers = pd.concat([data_timeweb_servers, config_row], axis=1, sort=False)
        counter += 1

    def unpack_disks_timeweb(disks):
        output = {
            'hdd': 0,
            'ssd': 0,
            'nvme': 0
        }
        disks = disks.split('+')
        if len(disks) != 1:
            disks[1] = disks[1][1:]
        for disk in disks:
            disk = disk.split(' ')
            if disk[3] == 'тб':
                size = int(disk[0]) * int(disk[2]) * 1000
            else:
                size = int(disk[0]) * int(disk[2])
            if disk[4] == 'hdd':
                output[disk[4]] = size
            elif disk[4] == 'ssd':
                output[disk[4]] = size
            else:
                output[disk[4]] = size
        return output['hdd'], output['ssd'], output['nvme']

    data_timeweb_servers = data_timeweb_servers.transpose()
    data_timeweb_servers['hdd_size'], data_timeweb_servers['ssd_size'], data_timeweb_servers['nvme_size'] = zip(
        *data_timeweb_servers['disks'].apply(unpack_disks_timeweb))
    data_timeweb_servers = data_timeweb_servers[['id', 'cpu_name', 'cpu_count', 'cores', 'frequency',
                                                 'ram', 'ddr4', 'ddr3', 'hdd_size', 'ssd_size', 'nvme_size', 'gpu',
                                                 'price', 'day', 'month', 'year']]
    data_timeweb_servers['frequency'] = data_timeweb_servers['frequency'].str.lower()
    data_timeweb_servers['frequency'] = data_timeweb_servers['frequency'].str.replace(r' ггц', '')
    data_timeweb_servers = data_timeweb_servers.astype(
        {'id': int, 'cpu_name': str, 'cpu_count': int, 'cores': int, 'frequency': float, 'ram': str, 'ddr4': int,
         'ddr3': int, 'hdd_size': int, 'ssd_size': int, 'nvme_size': int, 'gpu': str, 'price': float, 'day': int, 'month': int, 'year': int})
    engine = create_engine('postgresql://postgres:2320uhbR@127.0.0.1:5432/Competitor_analysis')
    data_timeweb_servers.to_sql('timeweb', engine, if_exists='append', index=False)


def load_reg_ru():
    r = requests.get('https://www.reg.ru/dedicated/')
    soup = BeautifulSoup(r.content, "html.parser")
    servers = soup.findAll('div', {'class': 'b-dedicated-servers-list-item'})
    data_reg_ru = pd.DataFrame()
    counter = 0
    for server in servers:
        id = server.find('div', class_='b-dedicated-servers-list-item__id').get_text().strip()
        cpu = server.find('span', class_='b-dedicated-servers-list-item__title').get_text().strip().split('сервера')[
            1].replace(r'Intel', '')
        parts = cpu.split('x')
        if len(parts) > 1:
            cpu_count = parts[0]
            cpu = parts[1]
        else:
            cpu_count = 1
        cores = server.find('span', class_='b-dedicated-servers-list-item__subtitle').get_text().strip()
        ram_data = server.find('div', class_='b-dedicated-servers-list-item__ram').get_text().strip().split('ГБ')
        ram = ram_data[0]
        if ram_data[1] == ' DDR4':
            ddr4 = 1
            ddr3 = 0
        else:
            ddr4 = 0
            ddr3 = 1

        disks = server.find('div', class_='b-dedicated-servers-list-item__hdd').decode_contents()
        if '<br/>' in str(disks):
            disks = str(disks)
            disks = re.sub('<br/>', ' | ', disks)
            disks = re.sub('<.*?>', '', disks)
            disks = disks.strip()
        else:
            #         disks = disks.get_text()
            disks = disks.strip()

        if server.find('div', class_='b-dedicated-servers-list-item__current-price') is None:
            price = server.find('div',
                                class_='b-dedicated-servers-list-item__price-value b-dedicated-servers-list-item__price-value_per-months_one').get_text().strip()
        else:
            price = server.find('div', class_='b-dedicated-servers-list-item__current-price').get_text().strip()
        datacenter = server.find('span', class_='b-dedicated-servers-list-item__address').get_text().strip()
        day = currentDay
        month = currentMonth
        year = currentYear
        config_row = [id, cpu, cpu_count, cores, ram, ddr4, ddr3, disks, datacenter, price, day, month, year]
        config_row = pd.Series(config_row,
                               index=['id', 'cpu_name', 'cpu_count', 'cores', 'ram', 'ddr4', 'ddr3', 'disks',
                                      'datacenter',
                                      'price','day', 'month', 'year'], name=counter)
        data_reg_ru = pd.concat([data_reg_ru, config_row], axis=1, sort=False)
        counter += 1

    data_reg_ru = data_reg_ru.transpose()
    data_reg_ru['id'] = data_reg_ru['id'].str.replace(r'Сервер', '')
    data_reg_ru['frequency'] = data_reg_ru['cores'].str.extract(r'(\d\.\d+)').astype(float)
    data_reg_ru['cores'] = data_reg_ru['cores'].str.extract(r'(\d+) ')
    data_reg_ru['ram'] = data_reg_ru['ram'].str.extract(r'(\d+) ')
    data_reg_ru['price'] = data_reg_ru['price'].str.extract(r'(\d+\s\d+)')
    data_reg_ru['price'] = data_reg_ru['price'].str.replace(r'\s', '')
    data_reg_ru['disks'] = data_reg_ru['disks'].str.lower()
    data_reg_ru['disks'] = data_reg_ru['disks'].str.replace(r'(sas|sata)', 'hdd')

    def unpack_disks(disks):
        output = {
            'hdd': 0,
            'ssd': 0,
            'nvme': 0
        }
        disks = re.findall(r'(\d+) x (\d+\.\d+|\d+) (тб|гб) (ssd|hdd|nvme)', disks)
        for disk in disks:
            if disk[2] == "тб":
                size = 1000 * int(disk[0]) * float(disk[1])
                output[disk[3]] = output[disk[3]] + size
            else:
                size = int(disk[0]) * float(disk[1])
                output[disk[3]] = output[disk[3]] + size
        return output['hdd'], output['ssd'], output['nvme']

    data_reg_ru['hdd_size'], data_reg_ru['ssd_size'], data_reg_ru['nvme_size'] = zip(
        *data_reg_ru['disks'].apply(unpack_disks))
    data_reg_ru = data_reg_ru[
        ['id', 'cpu_name', 'cpu_count', 'cores', 'frequency', 'ram', 'ddr4', 'ddr3', 'hdd_size', 'ssd_size',
         'nvme_size',
         'datacenter', 'price', 'day', 'month', 'year']]
    data_reg_ru = data_reg_ru.astype(
        {'id': str, 'cpu_name': str, 'cpu_count': int, 'cores': int, 'ram': int, 'ddr4': int, 'ddr3': int,
         'hdd_size': int,
         'ssd_size': int, 'nvme_size': int, 'datacenter': str, 'price': int, 'day': int, 'month': int, 'year': int})
    engine = create_engine('postgresql://postgres:2320uhbR@127.0.0.1:5432/Competitor_analysis')
    data_reg_ru.to_sql('reg_ru', engine, if_exists='append', index=False)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    url_hetzner = "https://www.hetzner.com/_resources/app/jsondata/live_data_en.json?m=1668706433366"
    url_timeweb = 'https://timeweb.cloud/v1.1/registration/servers'
    url_servers_ru = 'https://marketing-api.servers.ru/server_models?ru_site=true&gpu_option=0'
    url_hostkey = [
        "https://api.hostkey.com/v1/inv-api/get-presets-list?tag=bm&netag=web_noru,web_nosite&location=NL&currency=rub&pricerate=1&currencycon=br&servertype=1&filter=no&language=ru&invapi=yes",
        'https://api.hostkey.com/v1/inv-api/get-presets-list?tag=bm&netag=web_noru,web_nosite&location=US&currency=rub&pricerate=1&currencycon=br&servertype=1&filter=no&language=ru&invapi=yes',
        'https://api.hostkey.com/v1/inv-api/get-presets-list?tag=bm&netag=web_noru,web_nosite&location=RU&currency=rub&pricerate=1&currencycon=br&servertype=1&filter=no&language=ru&invapi=yes']

    #load_hostkey(url_hostkey)
    #load_timeweb(url_timeweb)
    #load_servers_ru(url_servers_ru)
    #load_reg_ru()
    #x = pd.read_csv(fr'D:\competitor_analysis\reg_ru\17_11_2022.csv')
    #y = pd.read_csv(fr'D:\competitor_analysis\reg_ru\19_11_2022.csv')
    # = pd.concat([x, y])
    # x.to_csv(fr'D:\competitor_analysis\hostkey\25_11_2022.csv', index=False)
    # z.to_csv(fr'D:\competitor_analysis\hostkey\26_11_2022.csv')
    #h = pd.read_csv(r'D:\competitor_analysis\hostkey\17-27.csv')
    #del h['competitor']
    engine = create_engine('postgresql://postgres:2320uhbR@127.0.0.1:5432/Competitor_analysis')
    df = pd.read_sql_query('select * from reg_ru', con=engine)
    print(df)
    #day = list()
    #for index,row in df.iterrows():
        #parts = row['date'].split('_')
        #day.append(int(parts[0]))
    #df['day'] = pd.Series(day)
    #df['month'] = 11
    #df['year'] = 2022
    #del df['date']
    #df.to_csv(fr'D:\competitor_analysis\hostkey\17-27.csv', index=False)
    #h.to_sql('hostkey', engine, if_exists='replace', index=False)



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
