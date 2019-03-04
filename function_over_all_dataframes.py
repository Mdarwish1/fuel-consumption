import pandas as pd
import re
import urllib
from sqlalchemy import create_engine, event, MetaData
from pyodbc import version as pyodbc_version
import struct
from datetime import datetime
import time


def handle_datetimeoffset(dto_value):
    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    # e.g., (2017, 3, 16, 10, 35, 18, 0, -6, 0)
    tup = struct.unpack("<6hI2h", dto_value)
    tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}".format(*tweaked)


class DBHandler(object):
    _engine = None
    _connection = None
    _metadata = None

    def __init__(self, db_params):
        """ argument DB : dictionary containing database connection definitions
        """
        params = urllib.parse.quote_plus("DRIVER={" + db_params['driver'] + "};SERVER=" +
                                         db_params['servername'] + ";DATABASE=" +
                                         db_params['database'] + ";UID=" +
                                         db_params['username'] + ";PWD={" +
                                         db_params['password'] + "}")

        self._engine = create_engine(
            db_params['drivername'] + ":///?odbc_connect=%s" % params)

        if pyodbc_version >= "4.0.23":
            @event.listens_for(self._engine, 'before_cursor_execute')
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                if executemany:
                    cursor.fast_executemany = True

    def connect(self):
        self._connection = self._engine.connect()
        self._metadata = MetaData(bind=self._engine)
        dbaip_conn = self._connection.connection
        dbaip_conn.add_output_converter(-155, handle_datetimeoffset)
        return self._connection


conf = {
    'drivername': 'mssql+pyodbc',
    'servername': '192.168.1.12',
    'port': '1433',
    'username': 'rubix',
    'password': 'rubix',
    'database': 'IPTDatalake',
    'driver': 'ODBC Driver 13 for SQL Server',
    'trusted_connection': 'yes',
    'legacy_schema_aliasing': False
}

dbhandler = DBHandler(conf)

with dbhandler.connect() as conn:
    df = pd.read_sql("""SELECT [Id]
      ,[SiteKey]
      ,[EngineTemperature]
      ,[DateTime]
      ,[FuelRate]
      ,[Power]
      ,[TankLevel]
     ,[ClientId]
      ,[DeviceId]
      ,[SiteId]
      ,[DateTimeFilter]
       FROM [TempLog].[Trion]
       Where SiteId = 20
      order by [DateTimeFilter]""", conn)


def my_situation(df, sensor_sens, max_consumption_rate):
    list = ['SiteId', 'SiteKey', 'DeviceId',
            'EngineTemperature', 'FuelRate', 'Power', 'TankLevel']
    df[list] = df[list].apply(pd.to_numeric, errors='coerce')
    df["TankLevelDiff"] = df['TankLevel'].astype(float).diff()
    df['TimeDiff'] = (pd.to_datetime(df.DateTimeFilter, errors='coerce',
                                     infer_datetime_format=True).diff().dt.seconds / 60).round(0)
    df['fuelRate'] = (60*df["TankLevelDiff"] / (df['TimeDiff'])).round(0)
    df['situation'] = df.index
    df['Power'] = df['Power'].astype(float)

    cond1 = (df['Power'] > 0)
    cond2 = (df['TankLevelDiff'] > 2*sensor_sens)
    cond3 = (df['Power'] == 0) & (df['TankLevelDiff'] <= -sensor_sens)
    cond4 = (df['Power'] > 0) & (df['TankLevelDiff'] >= -
                                 2*sensor_sens) & (df['TankLevelDiff'] < 0)
    cond5 = (df['Power'] > 0) & (df['TankLevelDiff'] >= 0)
    cond6 = (df['Power'] == 0) & (df['TankLevelDiff'] >= -
                                  sensor_sens) & (df['TankLevelDiff'] <= 2*sensor_sens)
    cond7 = (df['fuelRate'] < max_consumption_rate)

    df.loc[cond1, 'situation'] = 'c'
    df.loc[cond2, 'situation'] = 'f'
    df.loc[cond3, 'situation'] = 'c'
    df.loc[cond4, 'situation'] = 'c'
    df.loc[cond5, 'situation'] = 'i'
    df.loc[cond6, 'situation'] = 'i'
    df.loc[cond7, 'situation'] = 't'


def reg_expresfinditer(df, client, siteid, device, flag, input_regex):
    events_list = list()
    string_current = df.dropna(subset=['fuelRate'])[
        'situation'].str.cat(sep="")

    for match_nb, match in enumerate(re.finditer(input_regex, string_current)):
        start_date = df.loc[match.span()[0], 'DateTimeFilter']
        end_date = df.loc[match.span()[1], 'DateTimeFilter']
        if(match.span()[0] == match.span()[1]):
            quantity = df.loc[match.span()[1], 'TankLevelDiff']
        else:
            quantity = df.loc[match.span()[1], 'TankLevel'] - \
                df.loc[match.span()[0], 'TankLevel']

        meantemp = df.loc[match.span()[0]:match.span()[1],
                          'EngineTemperature'].mean()
        meanpower = df.loc[match.span()[0]:match.span()[1], 'Power'].mean()
        sitekey = df.loc[match.span()[0], 'SiteKey']
        events_list.append(
            {
                'DeviceId': device,
                'SiteId': siteid,
                'SiteKey': sitekey,
                'Event_Flag': flag,
                'StartDate': start_date,
                'EndDate': end_date,
                'Quantity_of_Fuel_changed': quantity,
                'Average Temperature': meantemp
            })
    return events_list


def reg_correction(df, input_regex):
    string_current = df.dropna(subset=['fuelRate'])[
        'situation'].str.cat(sep="")
    for match_nb, match in enumerate(re.finditer(input_regex, string_current)):
        df.at[match.span()[0]:match.span()[1], 'situation'] = 'i'

        df_event = pd.DataFrame(columns=['DeviceId', 'SiteId', 'SiteKey', 'Event_Flag',
                                         'StartDate', 'EndDate', 'Quantity_of_Fuel_changed', 'Average Temperature'])


filling_regex = r'[f]+'
theft_regex = r'[t]+'
consumption_regex = r'[c]+i{0,2}[c]+'
idle_regex = r'i{3,}'
df_modified = pd.DataFrame()

unique_clients = df['ClientId'].unique()
for client in unique_clients:
    unique_sites = df[df['ClientId'] == client]['SiteId'].unique()
    for site in unique_sites:
        unique_devices = df[df['SiteId'] == site]['DeviceId'].unique()
        for device in unique_devices:
            df_current = df[(df['SiteId'] == site) & (df['DeviceId'] == device) & (
                df['ClientId'] == client)].copy().reset_index()
            my_situation(df_current, 2, -8)
            df_modified = df_modified.append(df_current)
            if (len(reg_expresfinditer(df_current, client, site, device, 'filling', filling_regex)) > 0):
                df_event = df_event.append(reg_expresfinditer(
                    df_current, client, site, device, 'filling', filling_regex))
            if (len(reg_expresfinditer(df_current, client, site, device, 'theft', theft_regex)) > 0):
                df_event = df_event.append(reg_expresfinditer(
                    df_current, client, site, device, 'theft', theft_regex))
            if (len(reg_expresfinditer(df_current, client, site, device, 'consumption', consumption_regex)) > 0):
                df_event = df_event.append(reg_expresfinditer(
                    df_current, client, site, device, 'consumption', consumption_regex))
            if (len(reg_expresfinditer(df_current, client, site, device, 'idle', idle_regex)) > 0):
                df_event = df_event.append(reg_expresfinditer(
                    df_current, client, site, device, 'idle', idle_regex))


df_event = df_event.sort_values(by=['StartDate'])

writer = pd.ExcelWriter('C:\\Users\\Dana\\Documents\\excel\\output.xlsx')
df.to_excel(writer, 'data')
df_modified.to_excel(writer, 'modified_data')
df_event.to_excel(writer, 'events')
writer.save()
