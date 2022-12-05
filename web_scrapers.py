# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from datetime import date
import numpy
import pandas as pd
from uuid import uuid4
import requests
import re
from fake_headers import Headers
import time
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

current_date = date.today()


def load_data(url):
    header = Headers(headers=False)
    header = header.generate()
    try:
        servers = requests.get(url, headers=header, verify=False)
    except requests.exceptions.HTTPError as err:
        time.sleep(10)
        raise SystemExit(err)
    return servers


def add_to_db(data_from_website, provider):
    engine = create_engine('postgresql://postgres:2320uhbR@127.0.0.1:5432/Competitor_analysis')
    last_config = pd.read_sql_query(f"select * from configurations WHERE provider = '{provider}'", con=engine)
    list_columns = ['cpu_name', 'cpu_count', 'gpu', 'gpu_count', 'cores', 'frequency', 'ram', 'ddr4', 'ddr3',
                    'hdd_size', 'ssd_size', 'nvme_size', 'datacenter','provider']
    merge = data_from_website.merge(last_config, on=list_columns, how='left')
    merge = merge.rename(columns={'id_config_x': 'id_config', 'id_config_y': 'last'})
    price = merge.loc[~merge['last'].isna()]
    price = price[['id_config', 'price', 'date', 'last']]
    price['id_config'] = price['last']
    del price['last']
    new_data = merge.loc[merge['last'].isna()]
    if ~new_data.empty:
        new_config = new_data[['id_config', 'cpu_name', 'cpu_count', 'gpu', 'gpu_count', 'cores', 'frequency', 'ram',
                               'ddr4', 'ddr3', 'hdd_size', 'ssd_size', 'nvme_size', 'datacenter','provider']]
        new_config.to_sql('configurations', engine, if_exists='append', index=False)
        new_price = new_data[['id_config', 'price', 'date']]
        price = pd.concat([price, new_price])


def load_servers_ru(url):
    servers_ru = load_data(url).json()
    servers_ru = servers_ru['data']
    data_servers_ru = pd.DataFrame()
    counter = 0
    for server in servers_ru:
        id_config = uuid4()
        cpu_name = server['cpu_name']
        cpu_count = server['processor_count']
        gpu_count = server['gpu_count']
        if gpu_count is None:
            gpu_count = 0
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
        gpu = None
        datacenter = server['location_name'].split(' ')[0]
        competitor = 'servers_ru'
        price = server['prices']['full']['hosting']['total']
        date = current_date
        config_row = [id_config, cpu_name, cpu_count, gpu, gpu_count, cores, freq, ram, ddr4, ddr3, disks, datacenter,
                      competitor, price, date]
        config_row = pd.Series(config_row,
                               index=['id_config', 'cpu_name', 'cpu_count', 'gpu', 'gpu_count', 'cores', 'frequency',
                                      'ram', 'ddr4', 'ddr3', 'disks', 'datacenter', 'provider', 'price',
                                      'date'],
                               name=counter)

        data_servers_ru = pd.concat([data_servers_ru, config_row], axis=1, sort=False)
        counter += 1

    data_servers_ru = data_servers_ru.transpose()

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
        ['id_config', 'cpu_name', 'cpu_count', 'gpu', 'gpu_count', 'cores', 'frequency', 'ram', 'ddr4', 'ddr3',
         'hdd_size', 'ssd_size', 'nvme_size', 'datacenter', 'provider', 'price', 'date']]
    data_servers_ru = data_servers_ru.astype(
        {'id_config': object, 'cpu_name': str, 'cpu_count': int, 'gpu': None, 'gpu_count': int, 'cores': int,
         'frequency': float, 'ram': int, 'ddr4': int, 'ddr3': int,
         'hdd_size': int, 'ssd_size': int, 'nvme_size': int, 'datacenter': str, 'provider': str, 'price': float,
         'date': object})
    add_to_db(data_servers_ru, 'servers_ru')


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
            datacenter = server['common']['location']
            price = server['common']['conditions']['items'][0]['prices']['current']
            a = server['hardware']['cpu']['description']
            a = a.replace('xx*', ' ')
            # a = a.replace('X', '')
            a = a.split('x')
            if len(a) == 1:
                cpu_count = 1
                cpu_name = a[0].strip(r'\s')
            else:
                cpu_count = a[0]
                cpu_name = a[1].strip(r'\s')
            cores = server['hardware']['cpu']['number_cores']
            freq = server['hardware']['cpu']['items'][0]['ghz']
            ram = server['hardware']['ram']['volume']
            disks = server['hardware']['hard_drive']['description'].lower()
            id_config = uuid4()
            competitor = 'hostkey'
            date = current_date
            gpu = ddr3 = ddr4 = None
            gpu_count = 0
            config_row = [id_config, cpu_name, cpu_count, gpu, gpu_count, cores, freq, ram, ddr4, ddr3, disks,
                          datacenter,
                          competitor, price, date]

            config_row = pd.Series(config_row,
                                   index=['id_config', 'cpu_name', 'cpu_count', 'gpu', 'gpu_count', 'cores',
                                          'frequency',
                                          'ram', 'ddr4', 'ddr3', 'disks', 'datacenter', 'provider', 'price',
                                          'date'], name=counter)

            data_hostkey_servers = pd.concat([data_hostkey_servers, config_row], axis=1, sort=False)

            counter += 1

        data_hostkey_servers = data_hostkey_servers.transpose()
        data_hostkey_servers['hdd_size'], data_hostkey_servers['ssd_size'], data_hostkey_servers['nvme_size'] = zip(
            *data_hostkey_servers['disks'].apply(unpack_disks_hostkey))
        data_hostkey_servers = data_hostkey_servers[
            ['id_config', 'cpu_name', 'cpu_count', 'gpu', 'gpu_count', 'cores', 'frequency', 'ram', 'ddr4', 'ddr3',
             'hdd_size', 'ssd_size', 'nvme_size', 'datacenter', 'provider', 'price', 'date']]
        return data_hostkey_servers

    d_NL = create_df(url[0])
    d_USA = create_df(url[1])
    d_R = create_df(url[2])
    data = [d_NL, d_USA, d_R]
    res_hostkey = pd.concat(data)
    res_hostkey['cpu_name'] = res_hostkey['cpu_name'].str.strip()
    res_hostkey = res_hostkey.astype(
        {'id_config': object, 'cpu_name': str, 'cpu_count': int, 'gpu': None , 'gpu_count': int, 'cores': int,
         'frequency': float, 'ram': int, 'ddr4': None, 'ddr3': None,
         'hdd_size': int, 'ssd_size': int, 'nvme_size': int, 'datacenter': str, 'provider': str, 'price': float,
         'date': object})
    add_to_db(res_hostkey, 'hostkey')


def load_timeweb(url):
    timeweb_servers = load_data(url).json()
    timeweb_servers = timeweb_servers['body']
    data_timeweb_servers = pd.DataFrame()
    counter = 0
    for server in timeweb_servers:
        id_config = uuid4()
        a = server['cpu_vendor_short'].split('x')
        if len(a) == 1:
            cpu_count = 1
            cpu_name = a[0].strip().replace(r'Intel ', ' ')
        else:
            cpu_count = a[0]
            cpu_name = a[1].strip().replace(r'Intel ', ' ')
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
            gpu_data = parts[1]
            gpu_data_parts = gpu_data.split('x')
            if len(gpu_data_parts) > 2:
                gpu = (gpu_data_parts[1]+'x'+ gpu_data_parts[2]).strip()
                gpu_count = int(gpu_data_parts[0])
            else:
                gpu = gpu_data.strip()
                gpu_count = 1
            disks = parts[0]
        else:
            gpu = None
            gpu_count = 0
        price = server['price']
        date = current_date
        competitor = 'timeweb'
        datacenter = numpy.NAN
        config_row = [id_config, cpu_name, cpu_count, gpu, gpu_count, cores, freq, ram, ddr4, ddr3, disks, datacenter,
                      competitor, price, date]
        config_row = pd.Series(config_row,
                               index=['id_config', 'cpu_name', 'cpu_count', 'gpu', 'gpu_count', 'cores', 'frequency',
                                      'ram', 'ddr4', 'ddr3',
                                      'disks', 'datacenter', 'provider', 'price', 'date'], name=counter)

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
    data_timeweb_servers = data_timeweb_servers[
        ['id_config', 'cpu_name', 'cpu_count', 'gpu', 'gpu_count', 'cores', 'frequency', 'ram', 'ddr4', 'ddr3',
         'hdd_size', 'ssd_size', 'nvme_size', 'datacenter', 'provider', 'price', 'date']]
    data_timeweb_servers['frequency'] = data_timeweb_servers['frequency'].str.lower()
    data_timeweb_servers['frequency'] = data_timeweb_servers['frequency'].str.replace(r' ггц', '')
    data_timeweb_servers['cpu_name'] = data_timeweb_servers['cpu_name'].str.strip()

    data_timeweb_servers = data_timeweb_servers.astype(
        {'id_config': object, 'cpu_name': str, 'cpu_count': int, 'gpu': object, 'gpu_count': int, 'cores': int,
         'frequency': float, 'ram': int, 'ddr4': int, 'ddr3': int,
         'hdd_size': int, 'ssd_size': int, 'nvme_size': int, 'datacenter': None, 'provider': str, 'price': float,
         'date': object})
    add_to_db(data_timeweb_servers, 'timeweb')


def load_reg_ru():
    r = requests.get('https://www.reg.ru/dedicated/')
    soup = BeautifulSoup(r.content, "html.parser")
    servers = soup.findAll('div', {'class': 'b-dedicated-servers-list-item'})
    data_reg_ru = pd.DataFrame()
    counter = 0
    for server in servers:
        id_config = uuid4()
        cpu_name = server.find('span', class_='b-dedicated-servers-list-item__title').get_text().strip().split('сервера')[
            1].replace(r'Intel', '')
        parts = cpu_name.split('x')
        if len(parts) > 1:
            cpu_count = parts[0]
            cpu_name = parts[1]
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
        date = current_date
        gpu = None
        gpu_count = 0
        competitor = 'reg_ru'
        config_row = [id_config, cpu_name, cpu_count, gpu, gpu_count, cores, ram, ddr4, ddr3, disks, datacenter,
                      competitor, price, date]
        config_row = pd.Series(config_row,
                               index=['id_config', 'cpu_name', 'cpu_count', 'gpu', 'gpu_count', 'cores',
                                      'ram', 'ddr4', 'ddr3',
                                      'disks', 'datacenter', 'provider', 'price', 'date'], name=counter)
        data_reg_ru = pd.concat([data_reg_ru, config_row], axis=1, sort=False)
        counter += 1

    data_reg_ru = data_reg_ru.transpose()
    data_reg_ru['frequency'] = data_reg_ru['cores'].str.extract(r'(\d\.\d+)').astype(float)
    data_reg_ru['cpu_name'] = data_reg_ru['cpu_name'].str.strip()
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
        ['id_config', 'cpu_name', 'cpu_count', 'gpu', 'gpu_count', 'cores', 'frequency', 'ram', 'ddr4', 'ddr3',
         'hdd_size', 'ssd_size', 'nvme_size', 'datacenter', 'provider', 'price', 'date']]
    data_reg_ru = data_reg_ru.astype(
        {'id_config': object, 'cpu_name': str, 'cpu_count': int, 'gpu': None, 'gpu_count': int, 'cores': int,
         'frequency': float, 'ram': int, 'ddr4': int, 'ddr3': int,
         'hdd_size': int, 'ssd_size': int, 'nvme_size': int, 'datacenter': str, 'provider': str, 'price': float,
         'date': object})
    add_to_db(data_reg_ru, 'reg_ru')



