import psycopg2
from psycopg2 import sql, OperationalError
from psycopg2.extras import execute_batch


def get_postgres_connection(config):
    return psycopg2.connect(
        host=config['postgres']['host'],
        port=config['postgres']['port'],
        database=config['postgres']['database'],
        user=config['postgres']['user'],
        password=config['postgres']['password']
    )


def init_tables(conn, config):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ftp_parsed_data (
            id SERIAL PRIMARY KEY,
            stn VARCHAR(10),
            wban VARCHAR(10),
            yearmoda DATE,
            temp FLOAT,
            dewp FLOAT,
            slp FLOAT,
            stp FLOAT,
            visib FLOAT,
            wdsp FLOAT,
            mxspd FLOAT,
            gust FLOAT,
            max_temp FLOAT,
            min_temp FLOAT,
            prcp FLOAT,
            sndp FLOAT,
            frshtt VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS space_weather_data (
            id SERIAL PRIMARY KEY,
            type VARCHAR(20) NOT NULL,
            time TIMESTAMP,
            value FLOAT,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
    conn.commit()


def write_to_postgres(conn, table_name, records, columns):
    escaped_columns = [sql.Identifier(col.replace('"', '""')) for col in columns]

    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(escaped_columns),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )

    with conn.cursor() as cur:
        execute_batch(cur, query, records)
    conn.commit()
