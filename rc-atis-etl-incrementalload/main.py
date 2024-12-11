import sqlalchemy
import csv
from datetime import datetime, timedelta

from models import RaliwayCrossingData, RailwayCrossingDataRP, Base
from sqlalchemy import create_engine, text, insert, inspect, table
from sqlalchemy.orm import Session
from typing import List, Dict, Tuple
from tqdm.contrib import tzip
import xlrd

class ETL:
    def __init__(self, environment='local'):
        old_params = "DRIVER={SQL Server};SERVER=sqlprd.th.gov.bc.ca;DATABASE=CS_ArchiveServer;Trusted_Connection=yes"
        self.old_engine = create_engine(f'mssql+pyodbc:///?odbc_connect={old_params}')
        with self.old_engine.connect() as conn:
            table_names = conn.execute(text(
                ''' SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='CS_ArchiveServer'
                    ORDER BY TABLE_NAME asc'''
            )).fetchall()
            self.old_table_names = [table[0] for table in table_names]

        if environment == 'dev':
            new_params = "DRIVER={SQL Server};SERVER=Mara;DATABASE=RCATIS_TFM_DEV;Trusted_Connection=yes"
        elif environment == 'prod':
            new_params = "DRIVER={SQL Server};SERVER=Rosedale;DATABASE=RCATIS_TFM_PRD;Trusted_Connection=yes"
        else:
            new_params = "DRIVER={SQL Server};SERVER=localhost;DATABASE=RCATIS_TFM_DEV;Trusted_Connection=yes"

        self.new_engine = create_engine(f'mssql+pyodbc:///?odbc_connect={new_params}', use_setinputsizes=False)
        Base.metadata.create_all(self.new_engine)

    def get_latest_date(self):
        """Get the most recent date from the target database"""
        query = text('''
            SELECT MAX(created) as latest_date 
            FROM RailwayCrossingData
        ''')
        with self.new_engine.connect() as connection:
            result = connection.execute(query).fetchone()
            result = datetime(*xlrd.xldate_as_tuple(result[0], 0))
            return result if result else datetime(2000, 1, 1)  # Default to old date if no records

    def get_data(self, table_names:Tuple[str, str], latest_date: datetime):

        query_1 = text(f'''
            SELECT * 
            FROM {table_names[0]} 
        ''')
        query_2 = text(f'''
            SELECT * 
            FROM {table_names[1]} 
        ''')

        with self.old_engine.connect() as connection:
            result_1 = connection.execute(query_1).fetchall()
            result_2 = connection.execute(query_2).fetchall()

        table_date = f'{table_names[0][1:5]}-{table_names[0][7:9]}-{table_names[0][11:]}'
        if table_date == latest_date.strftime('%Y-%m-%d'):
            result_1 = [result for result in result_1 if
                        self.convert_date(result[2]) >= latest_date]
            result_2 = [result for result in result_2 if
                        self.convert_date(result[3]) >= latest_date]

        return result_1, result_2

    def write_data(self, data: tuple):
        rc_data = [item._mapping for item in data[0]]
        rp_data = [item._mapping for item in data[1]]

        rc_to_write: List[Dict] = [

            {
             'event_key': item.get('EventKey'),
             'created': item.get('Created'),
             'time_zone_bias': item.get('TimeZoneBias'),
             'server': item.get('Server'),
             'category': item.get('Category'),
             'description': item.get('Description'),
             'color': item.get('Color'),
             'priority': item.get('Priority'),
             'ack_date': item.get('AckDate'),
             'ack_user': item.get('AckUser'),
             'ack_comment': item.get('AckComment'),
             'script_tag': item.get('ScriptTag'),
             'meta_data': item.get('MetaData'),
             'url': item.get('URL'),
             'remote_url': item.get('RemoteURL'),
             'latitude': item.get('Latitude'),
             'longitude': item.get('Longitude'),
             'associated_devices': item.get('AssociatedDevices'),
             'ack_interval': item.get('AckInterval'),
             'owner_interval': item.get('OwnerInterval'),
             'last_owner': item.get('LastOwner'),
             'last_owner_date': item.get('LastOwnerDate')}
            for item in rc_data
        ]
        rp_to_write: List[Dict] = [

            {
             'name': item.get('Name'),
             'prop_value': item.get('PropValue'),
             'update_time': item.get('UpdateTime'),
             'initial_value': item.get('InitialValue'),
             'trend_id': item.get('TrendID'),
             'server': item.get('Server'),
             'read_prop_update_id': item.get('ReadPropUpdateID')}
            for item in rp_data
        ]

        with Session(self.new_engine) as session:
            session.execute(insert(RaliwayCrossingData), rc_to_write)
            session.execute(insert(RailwayCrossingDataRP), rp_to_write)
            session.commit()

    def process(self):
        latest_datetime = self.get_latest_date()
        latest_date = latest_datetime.strftime('%Y-%m-%d')
        table_names = [t for t in self.old_table_names if not t.endswith('_RP')]
        table_names = [t for t in table_names if latest_date <= f'{t[1:5]}-{t[7:9]}-{t[11:]}']
        table_names.sort()
        rp_tables = [t for t in self.old_table_names if t.endswith('_RP')]
        rp_tables = [t for t in rp_tables if latest_date <= f'{t[1:5]}-{t[7:9]}-{t[11:]}']
        rp_tables.sort()

        for tables in tzip(table_names, rp_tables):
            try:
                rc_data, rc_rp_data = self.get_data(tables, latest_datetime)
            except Exception as e:
                print(e)
                continue
                # Data Processing steps go here.
            self.write_data((rc_data, rc_rp_data))

    @staticmethod
    def convert_date(serial_date) -> datetime:
        converted = datetime(*xlrd.xldate_as_tuple(serial_date, 0))
        return converted



if __name__ == '__main__':
    etl = ETL(environment='dev')
    etl.process()