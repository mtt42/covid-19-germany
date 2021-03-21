# Setup -----------------------------------------------------------------------

# full packages
import os
import yaml
import pandas as pd
import config
import db_helper

# functions
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from db_helper import pg_get_query

# constants paths and files
DIR_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIR_CACHE = DIR_PROJECT_ROOT + "/cache/"

FILE_CONFIG = DIR_PROJECT_ROOT + "/dev.cfg"
FILE_SOURCE = "https://pavelmayer.de/covid/risks/all-series.csv"
FILE_S3 = "s3://uniformatics/data/covid19-pavel-mayer/all-series.csv"
FILE_S3_ETL = "s3://uniformatics/data/covid19-pavel-mayer/all-series_etl.csv"

FILE_EXTENSION = ".csv"
FILE_SUFFIX = "_all_series"

DB_SYSTEM = "dev"
# constants postgres db

cfg = config.Config(FILE_CONFIG)

DB_USER = cfg['user']
DB_PWD = cfg['password']
DB_HOST = cfg['host']
DB_NAME = cfg['dbname']
DB_CONN_STRING = f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:5432/{DB_NAME}"

# variables
today_as_string = str(datetime.today().date())
yesterday_as_string = str(datetime.today().date() - timedelta(days=1))

file_name_new = DIR_CACHE + today_as_string + FILE_SUFFIX + FILE_EXTENSION
file_name_old = DIR_CACHE + yesterday_as_string + FILE_SUFFIX + FILE_EXTENSION
file_name_etl = DIR_CACHE + today_as_string + "_all_series_etl" + FILE_EXTENSION

# Import and Save Data --------------------------------------------------------

if not os.path.exists(file_name_new):
    df_all_series = pd.read_csv(FILE_SOURCE)
    df_all_series["Datum"] = pd.to_datetime(df_all_series["Datum"], format="%d.%m.%Y")
    df_all_series.to_csv(file_name_new)
    if os.path.exists(file_name_old):
        os.remove(file_name_old)
else:
    df_all_series = pd.read_csv(file_name_new)

# Push data as is to staging layer of Postgres DB ------------------------------

pg_conn = create_engine(DB_CONN_STRING)
df_all_series.to_sql('covid19_rki', pg_conn, schema="staging", if_exists="replace")

# Backup on S3 space -----------------------------------------------------------

os.system(f"s3cmd put {file_name_new} {FILE_S3}")

# Get ETL results --------------------------------------------------------------

f = open(DIR_PROJECT_ROOT + "/queries/etl_covid19.sql")
query = f.read()
f.close()

df = pg_get_query(query, FILE_CONFIG)
df.to_csv(file_name_etl)

os.system(f"s3cmd put {file_name_etl} {FILE_S3_ETL}")
