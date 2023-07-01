from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import datetime
import time
import copy

import sqlite3
import pymysql

URL_domain = 'https://meteo.arso.gov.si'
URL_index = '/uploads/probase/www/observ/surface/text/sl/observation_si/index.html'
PARAMS_XML = {
    'Ime lokacije': 'domain_longTitle',
    'Geografska dolžina': 'domain_lat',
    'Geografska širina': 'domain_lon',
    'Nadmorska višina': 'domain_altitude',
    'Čas meritve': 'tsValid_issued_UTC',
    # 'Ocenjena oblačnost': 'nn_icon-wwsyn_icon',
    # 'Pojavi': 'wwsyn_icon',
    'Temperatura': 't',
    'Temperatura rosišča': 'td',
    'Povprečna temperatura v časovnem intervalu': 'tavg',
    'Maksimalna temperatura v časovnem intervalu': 'tx',
    'Minimalna temperatura v časovnem intervalu': 'tn',
    'Relativna vlažnost': 'rh',
    'Povprečna relativna vlažnost v časovnem intervalu': 'rhavg',
    'Smer vetra': 'dd_val',
    'Povprečna smer vetra v časovnem intervalu': 'ddavg_val',
    'Smer največjega sunka vetra v časovnem intervalu': 'ddmax_val',
    'Hitrost vetra': 'ff_val',
    'Povprečna hitrost vetra v časovnem intervalu': 'ffavg_val',
    'Maksimalna hitrost vetra v časovnem intervalu': 'ffmax_val',
    'Zračni tlak reduciran na morski nivo': 'msl',
    'Povprečni zračni tlak v časovnem intervalu reduciran na morski nivo': 'mslavg',
    'Zračni tlak na lokaciji': 'p',
    'Povprečni zračni tlak na lokaciji v časovnem intervalu': 'pavg',
    'Vsota padavin v časovnem intervalu': 'rr_val',
    'Višina snežne odeje': 'snow',
    '1-urne padavine': 'tp_1h_acc',
    'Vsota padavin od 6 oz. 18 UTC dalje,': 'tp_12h_acc',
    '24-urna vsota padavin': 'tp_24h_acc',
    'Temperatura vode': 'tw',
    'Globalno sončno obsevanje': 'gSunRad',
    'Povprečno globalno sončno obsevanje v časovnem intervalu': 'gSunRadavg',
    'Difuzno sončno obsevanje': 'diffSunRad',
    'Povprečno difuzno sončno obsevanje v časovnem intervalu': 'diffSunRadavg',
    'Temperatura na 5 cm': 't_5_cm',
    'Povprečna temperatura na 5 cm': 'tavg_5_cm',
    'Temperatura tal v globini 5 cm': 'tg_5_cm',
    'Povprečna temperatura tal v časovnem intervalu v globini 5 cm': 'tgavg_5_cm',
    'Temperatura tal v globini 10 cm': 'tg_10_cm',
    'Povprečna temperatura tal v časovnem intervalu v globini 10 cm': 'tgavg_10_cm',
    'Temperatura tal v globini 20 cm': 'tg_20_cm',
    'Povprečna temperatura tal v časovnem intervalu v globini 20 cm': 'tgavg_20_cm',
    'Temperatura tal v globini 30 cm': 'tg_30_cm',
    'Povprečna temperatura tal v časovnem intervalu v globini 30 cm': 'tgavg_30_cm',
    'Temperatura tal v globini 50 cm': 'tg_50_cm',
    'Povprečna temperatura tal v časovnem intervalu v globini 50 cm': 'tgavg_50_cm',
    'Višina oblačnih slojev 1,': 'hhs1_val',
    'Višina oblačnih slojev 2,': 'hhs2_val',
    'Višina oblačnih slojev 3,': 'hhs3_val',
    'Višina oblačnih slojev 4,': 'hhs4_val',
    'Količina oblačnosti po slojih 1,': 'ns1_val',
    'Količina oblačnosti po slojih 2,': 'ns2_val',
    'Količina oblačnosti po slojih 3,': 'ns3_val',
    'Količina oblačnosti po slojih 4,': 'ns4_val'}
PARAMS_DB = {
    'Ime lokacije': ('station_name', 'TEXT NOT NULL', 'VARCHAR(200) NOT NULL'),
    'Geografska dolžina': ('longitude', 'REAL NOT NULL', 'FLOAT(7,4) NOT NULL'),
    'Geografska širina': ('latitude', 'REAL NOT NULL', 'FLOAT(7,4) NOT NULL'),
    'Nadmorska višina': ('altitude', 'INTEGER NOT NULL', 'SMALLINT NOT NULL'),
    'Čas meritve': ('meas_time', 'TEXT NOT NULL', 'VARCHAR(50) NOT NULL'),
    # 'Ocenjena oblačnost': ('estim_cloud', 'TEXT', 'VARCHAR(100)'),
    # 'Pojavi': ('phenom', 'TEXT', 'VARCHAR(100)'),
    'Temperatura': ('temperature', 'REAL', 'FLOAT(3,1)'),
    'Temperatura rosišča': ('temp_dew_point', 'REAL', 'FLOAT(3,1)'),
    'Povprečna temperatura v časovnem intervalu': ('avg_temp', 'REAL', 'FLOAT(3,1)'),
    'Maksimalna temperatura v časovnem intervalu': ('max_temp', 'REAL', 'FLOAT(3,1)'),
    'Minimalna temperatura v časovnem intervalu': ('min_temp', 'REAL', 'FLOAT(3,1)'),
    'Relativna vlažnost': ('rel_humid', 'INTEGER', 'SMALLINT'),
    'Povprečna relativna vlažnost v časovnem intervalu': ('rel_humid_avg', 'INTEGER', 'SMALLINT'),
    'Smer vetra': ('wind_dir', 'INTEGER', 'SMALLINT'),
    'Povprečna smer vetra v časovnem intervalu': ('wind_dir_avg', 'INTEGER', 'SMALLINT'),
    'Smer največjega sunka vetra v časovnem intervalu': ('wind_dir_max', 'INTEGER', 'SMALLINT'),
    'Hitrost vetra': ('wind_veloc', 'REAL', 'FLOAT(3,1)'),
    'Povprečna hitrost vetra v časovnem intervalu': ('wind_veloc_avg', 'REAL', 'FLOAT(3,1)'),
    'Maksimalna hitrost vetra v časovnem intervalu': ('wind_veloc_avg', 'REAL', 'FLOAT(3,1)'),
    'Zračni tlak reduciran na morski nivo': ('press_at_0', 'REAL', 'FLOAT(3,1)'),
    'Povprečni zračni tlak v časovnem intervalu reduciran na morski nivo': ('press_at_0_avg', 'REAL', 'FLOAT(3,1)'),
    'Zračni tlak na lokaciji': ('press_loc', 'REAL', 'FLOAT(3,1)'),
    'Povprečni zračni tlak na lokaciji v časovnem intervalu': ('press_loc_avg', 'REAL', 'FLOAT(3,1)'),
    'Vsota padavin v časovnem intervalu': ('precip', 'INTEGER', 'SMALLINT'),
    'Višina snežne odeje': ('snow', 'INTEGER', 'SMALLINT'),
    '1-urne padavine': ('precip_1h', 'INTEGER', 'SMALLINT'),
    'Vsota padavin od 6 oz. 18 UTC dalje,': ('prec_12h', 'INTEGER', 'SMALLINT'),
    '24-urna vsota padavin': ('prec_24h', 'INTEGER', 'SMALLINT'),
    'Temperatura vode': ('temp_water', 'REAL', 'FLOAT(3,1)'),
    'Globalno sončno obsevanje': ('sun_rad', 'REAL', 'FLOAT(3,1)'),
    'Povprečno globalno sončno obsevanje v časovnem intervalu': ('sun_rad_avg', 'REAL', 'FLOAT(3,1)'),
    'Difuzno sončno obsevanje': ('sun_rad_diff', 'REAL', 'FLOAT(3,1)'),
    'Povprečno difuzno sončno obsevanje v časovnem intervalu': ('sun_rad_diff_avg', 'REAL', 'FLOAT(3,1)'),
    'Temperatura na 5 cm': ('temp_5_cm', 'REAL', 'FLOAT(3,1)'),
    'Povprečna temperatura na 5 cm': ('temp_avg_5_cm', 'REAL', 'FLOAT(3,1)'),
    'Temperatura tal v globini 5 cm': ('temp_ground_5_cm', 'REAL', 'FLOAT(3,1)'),
    'Povprečna temperatura tal v časovnem intervalu v globini 5 cm': ('temp_ground_avg_5_cm', 'REAL', 'FLOAT(3,1)'),
    'Temperatura tal v globini 10 cm': ('temp_ground_10_cm', 'REAL', 'FLOAT(3,1)'),
    'Povprečna temperatura tal v časovnem intervalu v globini 10 cm': ('temp_ground_avg_10_cm', 'REAL', 'FLOAT(3,1)'),
    'Temperatura tal v globini 20 cm': ('temp_ground_20_cm', 'REAL', 'FLOAT(3,1)'),
    'Povprečna temperatura tal v časovnem intervalu v globini 20 cm': ('temp_ground_avg_20_cm', 'REAL', 'FLOAT(3,1)'),
    'Temperatura tal v globini 30 cm': ('temp_ground_30_cm', 'REAL', 'FLOAT(3,1)'),
    'Povprečna temperatura tal v časovnem intervalu v globini 30 cm': ('temp_ground_avg_30_cm', 'REAL', 'FLOAT(3,1)'),
    'Temperatura tal v globini 50 cm': ('temp_ground_50_cm', 'REAL', 'FLOAT(3,1)'),
    'Povprečna temperatura tal v časovnem intervalu v globini 50 cm': ('temp_ground_avg_50_cm', 'REAL', 'FLOAT(3,1)'),
    'Višina oblačnih slojev 1,': ('cloud_cov_1', 'TEXT', 'VARCHAR(100)'),
    'Višina oblačnih slojev 2,': ('cloud_cov_2', 'TEXT', 'VARCHAR(100)'),
    'Višina oblačnih slojev 3,': ('cloud_cov_3', 'TEXT', 'VARCHAR(100)'),
    'Višina oblačnih slojev 4,': ('cloud_cov_4', 'TEXT', 'VARCHAR(100)'),
    'Količina oblačnosti po slojih 1,': ('cloud_amo_1', 'TEXT', 'VARCHAR(100)'),
    'Količina oblačnosti po slojih 2,': ('cloud_amo_2', 'TEXT', 'VARCHAR(100)'),
    'Količina oblačnosti po slojih 3,': ('cloud_amo_3', 'TEXT', 'VARCHAR(100)'),
    'Količina oblačnosti po slojih 4,': ('cloud_amo_4', 'TEXT', 'VARCHAR(100)')}

STORE_INTO_SQLITE = True
STORE_INTO_MYSQL = False
STORE_INTO_CSV = True


def find_parameters(url):
    '''Preliminary search for xml tags'''

    r = requests.get(url)
    soup = bs(r.text, features="xml")
    result = soup.find('metData').find_all(lambda tag: '_var_desc' in tag.name)
    values = [tag.text for tag in result]
    names = [tag.name for tag in result]
    names_m = [name.split('_')[0] for name in names]
    options = dict(zip(values, names_m))
    return options


def get_stations():
    '''Fetch links to each station's xml subpage, store specified values into result_dict.'''

    r = requests.get(URL_domain + URL_index)
    soup = bs(r.content, 'html.parser')
    res = soup.find_all('table', {'class': 'meteoSI-table'})[2].find('tbody').find_all('tr')
    result_dict = dict()
    for row in res[3:]:
        cols = row.find_all('td')
        name = cols[0].text
        link = cols[1].find('a').get('href')
        result_dict[name] = link

    return result_dict


def extract_latest(variables, locs):
    '''Takes locations and fetches data for specified parameters.
    Returns as pandas DataFrame.'''

    output = pd.DataFrame()
    # curr_timestamp = None
    for loc, loc_url in locs.items():
        r = requests.get(URL_domain + loc_url)
        soup = bs(r.text, features='xml').find('metData')
        res = dict()
        # print(station)
        for variable in variables:
            try:
                res[variable] = soup.find(PARAMS_XML[variable]).text
            except KeyError:
                print('Unknown variable. Please check variables list.')
            except AttributeError:
                print(f'!!! Cannot find value of {variable} for station {loc}.')

        new_row = pd.Series(data=res)
        new_station = pd.DataFrame(new_row, columns=[loc]).T
        output = pd.concat([output, new_station], axis=0)

    curr_timestamp = datetime.datetime.now()
    return curr_timestamp, output


def store_to_csv(df, file, **kwargs):
    '''Takes the pandas DataFile, stores to a csv file in append mode.'''
    df.to_csv(file, sep='\t', mode='a', **kwargs)


def store_to_sqlite(df, table_name):
    '''Defines the table if it doesn't exist,
    stores the values from the pandas DataFrame into table in SQLite database.'''

    params = df.columns.tolist()

    conn = sqlite3.connect('outputs/sample_sqlite.db')
    cur = conn.cursor()
    sql_command = f'CREATE TABLE IF NOT EXISTS {table_name} ('
    sql_command += 'id INTEGER PRIMARY KEY AUTOINCREMENT,'
    for param in params:
        sql_command += f'{PARAMS_DB[param][0]} {PARAMS_DB[param][1]},'
    sql_command = sql_command[:-1] + ');'

    try:
        cur.execute(sql_command)

        df = df.reset_index(drop=True)
        param_list = [PARAMS_DB[param][0] for param in params]

        for row in df.values.tolist():
            sql_command = f'INSERT INTO {table_name} {tuple(param_list)} VALUES {tuple(row)};'
            cur.execute(sql_command)
    except sqlite3.Error as e:
        conn.rollback()
        print(e)
    else:
        conn.commit()
    finally:
        conn.close()


def store_to_mysql(df, table_name):
    '''Defines the table if it doesn't exist,
    stores the values from the pandas DataFrame into table in mySQL database.'''

    params = df.columns.tolist()

    conn = pymysql.connect(host='localhost',
                           user='user',
                           password='pwd',
                           db='mysql_database')
    cur = conn.cursor()
    sql_command = f'CREATE TABLE IF NOT EXISTS {table_name} ('
    sql_command += 'id MEDIUMINT NOT NULL AUTO_INCREMENT,'
    for param in params:
        sql_command += f'{PARAMS_DB[param][0]} {PARAMS_DB[param][2]},'
    sql_command += 'PRIMARY KEY (id) );'

    cur.execute(sql_command)

    df = df.reset_index(drop=True)
    param_list = [PARAMS_DB[param][0] for param in params]

    for row in df.values.tolist():
        sql = f'INSERT INTO {table_name} {tuple(param_list)} VALUES {tuple(row)};'
        cur.execute(sql)
        conn.commit()

    conn.close()


def main():
    # file_name = 'outputs/sample_csv.csv'
    features_list = ['Ime lokacije', 'Geografska dolžina', 'Geografska širina', 'Čas meritve', 'Relativna vlažnost',
                     'Temperatura', 'Temperatura rosišča']

    station_list = ['Babno Polje (756 m)', 'Davča (1001 m)', 'Ilirska Bistrica', 'Krajinski park Goričko',
                    'Letališče Edvarda Rusjana Maribor', 'Malkovec', 'Novo mesto', 'Postojna (538 m)',
                    'Sevno (556 m)', 'Trbovlje', 'Vršič (1684 m)']

    stations = get_stations()

    if len(station_list) == 0:
        # If station list is empty, fetch data for all stations.
        station_selection = copy.deepcopy(stations)
    else:
        station_selection = {stat: stations[stat] for stat in station_list}

    if len(features_list) == 0:
        # If feature list is empty, fetch data for all features in PARAMS_XML.
        features_selection = list(PARAMS_XML.keys())
    else:
        features_selection = copy.deepcopy(features_list)

    # First sample, serves as reference
    sample_datetime, sample_df = extract_latest(features_selection, station_selection)
    if STORE_INTO_SQLITE:
        store_to_sqlite(sample_df, 'measurements')

    if STORE_INTO_MYSQL:
        store_to_mysql(sample_df, 'measurements')

    if STORE_INTO_CSV:
        store_to_csv(sample_df, 'outputs/sample_csv.csv', header=True)

    while True:
        sample_datetime_refresh, sample_df_refresh = extract_latest(features_selection, station_selection)
        diff = sample_datetime_refresh - sample_datetime

        # Compare time difference and writes if enough time has passed (1 hour);
        # Doesn't guarantee a fresh measurement as sometimes the web page can fail to refresh.
        if diff.seconds / 3600 > 1:
            print(f"Sample timestamp: {sample_datetime.strftime('%Y-%m-%d, %H:%M:%S')}")
            print(f"Refreshed timestamp: {sample_datetime_refresh.strftime('%Y-%m-%d, %H:%M:%S')}")

            if STORE_INTO_SQLITE:
                store_to_sqlite(sample_df_refresh, 'measurements')

            if STORE_INTO_MYSQL:
                store_to_mysql(sample_df_refresh, 'measurements')

            if STORE_INTO_CSV:
                store_to_csv(sample_df_refresh, 'outputs/sample_csv.csv', header=False)

            # After data is stored, must replace the timestamp and start from beginning. Purge df.
            sample_datetime = copy.deepcopy(sample_datetime_refresh)
            sample_datetime_refresh = pd.DataFrame()

        else:
            print(f'''{sample_datetime_refresh.strftime('%Y-%m-%d, %H:%M:%S')}: Sample timestamp is the same as before.
                Not refreshing data, waiting 10 minutes...''')
            time.sleep(600)


if __name__ == '__main__':
    main()
