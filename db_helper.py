import pandas as pd
import psycopg2 as pg
import config
import sys


def pg_get_query(sql, config_file="dev.cfg"):
    """
    :param sql: Query for which the results are returned
    :param config_file: dictionary of db configuration
    :return: results of sql as pandas dataframe
    """

    cfg = config.Config(config_file).as_dict()
    con = None

    try:
        # connect to the PostgreSQL database
        con = pg.connect(**cfg)
        # load results into pandas df
        df = pd.read_sql_query(sql=sql, con=con)
        # return pandas df
        return df

    except (Exception, pg.DatabaseError) as error:
        print('Damn it!', error)

    finally:
        if con is not None:
            con.close()
