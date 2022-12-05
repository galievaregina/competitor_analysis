from web_scrapers import load_hostkey,load_timeweb,load_reg_ru,load_servers_ru

if __name__ == '__main__':
    url_hetzner = "https://www.hetzner.com/_resources/app/jsondata/live_data_en.json?m=1668706433366"
    url_timeweb = 'https://timeweb.cloud/v1.1/registration/servers'
    url_servers_ru = 'https://marketing-api.servers.ru/server_models?ru_site=true&gpu_option=0'
    url_hostkey = [
        "https://api.hostkey.com/v1/inv-api/get-presets-list?tag=bm&netag=web_noru,web_nosite&location=NL&currency=rub&pricerate=1&currencycon=br&servertype=1&filter=no&language=ru&invapi=yes",
        'https://api.hostkey.com/v1/inv-api/get-presets-list?tag=bm&netag=web_noru,web_nosite&location=US&currency=rub&pricerate=1&currencycon=br&servertype=1&filter=no&language=ru&invapi=yes',
        'https://api.hostkey.com/v1/inv-api/get-presets-list?tag=bm&netag=web_noru,web_nosite&location=RU&currency=rub&pricerate=1&currencycon=br&servertype=1&filter=no&language=ru&invapi=yes']
    load_hostkey(url_hostkey)
    load_timeweb(url_timeweb)
    load_servers_ru(url_servers_ru)
    load_reg_ru()