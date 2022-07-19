import pymssql
import logging
import pandas as pd
from clickhouse_driver import Client
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
from msal import PublicClientApplication

_log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(logging.Formatter(_log_format))

logger.addHandler(stream_handler)
class Sql:
    """
    Класс для работы c базами данных по средстам sql-запросов.
    """

    @staticmethod
    def read_psql(con_str: str, query: str) -> pd.DataFrame:
        """
        Метод необходим для получения данных из выбранной базы данных.
        Пример:
            from app.dash_apps.common.db_resours import Sql, Config
            query = "select * from table;"
            df = Sql.read_psql(Config.PG_***, query)

        :param con_str: строка для подключения к базе данных.
        :param query: текст запроса.
        :return: таблица DataFrame с полученными данными из запроса.
        """
        con = create_engine(con_str)
        df = pd.read_sql_query(sql=query, con=con)
        con.dispose()

        return df

    @staticmethod
    def write_psql(con_str: str, query: str) -> bool:
        """
        Метод необходим для изменения данных в выборанной базе данных.
        Пример:
            from app.dash_apps.common.db_resours import Sql, Config
            query = "UPDATE table set value = 3 WHERE id = 1;"
            result = Sql.write_psql(Config.PG_***, query)

        :param con_str: строка для подключения к базе данных.
        :param query: текст запроса.
        :return: результат изменения.
        """
        con = create_engine(con_str)
        with con.connect() as connection:
            connection.execute(query)
        con.dispose()

        return True

    @staticmethod
    def write_psql_replace(
            con_str: str,
            name_schema: str,
            name_table: str,
            df: pd.DataFrame,
            dtype=None,
            chunksize=10240,
            index=False
    ) -> None:

        """
        Метод необходим для перезаписывания таблицы без её полного удаления.
        :param con_str:  строка для подключения к базе данных.
        :param name_schema:  Наименование схемы в бд.
        :param name_table:  Наименование таблицы в бд.
        :param df:  таблица с данными, которые необходимо загрузить в таблицу.
        :param dtype:  тип столбцов.
        :param chunksize:  количество байт в одном блоке.
        :param index:  Если true , то индексы строк тоже подгрузятся в бд.
        """

        if dtype is None:
            dtype = dict()

        con = create_engine(con_str)

        try:
            with con.connect() as connection:
                is_exists = connection.execute(
                    """
                    select count(*)
                    from pg_tables
                    where tablename='{name_table}' and schemaname='{name_schema}';
                    """.format(name_table=name_table, name_schema=name_schema)
                ).first()[0]

            if is_exists:
                with con.connect() as connection:
                    connection.execute(f'truncate "{name_schema}"."{name_table}";')

            df.to_sql(
                name=name_table,
                con=con,
                schema=name_schema,
                if_exists='append',
                chunksize=chunksize,
                index=index,
                dtype=dtype
            )
        finally:
            con.dispose()


def execute_mssql(query, connection_id):
    """Функция обращения к реплике 1с на чтение
    Параметры:
    query - строка, SQL запрос
    connection_id - название подключения в Airflow
    Возвращает - pandas DataFrame с содержанием результата выполнения запроса, названия столбцов из запроса"""
    connection = BaseHook.get_connection(connection_id)

    if connection.conn_type != "mssql":
        raise Exception("Тип подключения не MS SQL SERVER")

    conn = pymssql.connect(server=connection.host, user=connection.login, password=connection.password, database=connection.schema, timeout=1200)

    df = pd.read_sql(query, conn)

    return df


def execute_clickhouse(query, conn_id, external_tables=None) -> pd.DataFrame:
    """
    Функция для выполнения запросов к Clickhouse

    **Аргументы:**

    - `query: str` - запрос
    - `database: str` - база данных (по-умолчанию `'dns_log'`)
    - `external_tables: pandas.DataFrame` - датафрэйм из которого бдует генерироваться внешняя таблица

    **Возвращает:**
    >    `pandas.DataFrame` с результатом запроса
    """
    connection = BaseHook.get_connection(conn_id)

    client = Client(
        host=connection.host,
        port=connection.port,
        database=connection.schema,
        user=connection.login,
        password=connection.password
    )
    # if external_tables:
    #     external_tables = [df_to_table(table[0], table[1]) for table in external_tables]

    rv = client.execute(query, external_tables=external_tables, with_column_types=True)
    df = pd.DataFrame(rv[0])
    df.columns = [i[0] for i in rv[1]]
    return df

def get_client_clickhouse(conn_id) -> pd.DataFrame:
    """
    Функция для клиента Clickhouse с помощью которого можно выполнять запросы к базе данных

    **Аргументы:**
        conn_id - имя соединения

    **Возвращает:**
        client ClickHouse
    """
    connection = BaseHook.get_connection(conn_id)
    logger.info('Данные соединения получены')
    client = Client(
        host=connection.host,
        port=connection.port,
        database=connection.schema,
        user=connection.login,
        password=connection.password
    )
    return client

def get_postgres_connection_string(connection_id) -> str:
    """
    Возвращает строку подключения к postgres для указанного подключения
    :return:
    """
    connection = BaseHook.get_connection(connection_id)
    if connection.conn_type != "postgres":
        raise Exception('Тип подключения не postgres')
    return f'postgresql+psycopg2://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}'


def get_1c_basic_authentication_token() -> str:
    """
    Возвращает токен для аутентификации в API 1с
    :return:
    """
    connection = BaseHook.get_connection("1c_basic_authentication_token")
    return connection.password

def get_powerbi_access_token() -> str:
    """Получает access token для powerbi api

    Возвращает:
        str - токен доступа

    Исключения:
        - KeyError: в случае, если не был получен access token из запроса
        - Exception: в случае, если не был отправлен запрос на получение токена
    """

    powerbi_con = BaseHook.get_connection("powerbi_access_token")

    powerbi_user = powerbi_con.login
    powerbi_pass = powerbi_con.password
    powerbi_client_id = powerbi_con.schema
    powerbi_scope = "https://analysis.windows.net/powerbi/api/.default"
    # powerbi_scope = 'https://graph.microsoft.com/.default'
    powerbi_authority = "https://login.microsoftonline.com/organizations"

    logger.info(powerbi_client_id)
    response = None
    try:
        clientapp = PublicClientApplication(powerbi_client_id, authority=powerbi_authority) # client_credential=powerbi_secret)
        # response = clientapp.acquire_token_for_client(scopes=[powerbi_scope])
        accounts = clientapp.get_accounts(username=powerbi_user)
        logger.info(accounts)
        if accounts:
            logger.info('acc')
            response = clientapp.acquire_token_silent(powerbi_scope, account=accounts[0])
        
        if not response:
            logger.info('not res')
            response = clientapp.acquire_token_by_username_password(powerbi_user, powerbi_pass, scopes=[powerbi_scope])

        if "access_token" in response:
            logger.info(f"response['access_token'] c токеном")
        else:
            logger.info(response.get("error"))
            logger.info(response.get("error_description"))
            logger.info(response.get("correlation_id"))

        try:
            res = response['access_token']
            if isinstance(res, str):
                logger.info('Токен доступа получен')
                return res
            logger.error(ValueError(f"{res} has type {type(res)}"))
            raise ValueError(f"{res} has type {type(res)}")
        except KeyError as ex:
            logger.error(ex)
            raise ex

    except Exception as ex:
        logger.error(ex)
        raise ex

def execute_pg_hr(query,connection_id='mart_hr') -> str:
    """Функция обращения к хранилищу mart_hr DNS на чтение
    Параметры:
    query - строка, SQL запрос
    Возвращает - pandas DataFrame с содержанием результата выполнения запроса, названия столбцов из запроса"""

    connection = BaseHook.get_connection(connection_id)
    if connection.conn_type != "postgres":
        raise Exception('Тип подключения не postgres')
        
    conection_url = f'postgresql+psycopg2://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}'
    
    engine = create_engine(conection_url)
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df