import pandas as pd
import logging

from airflow import DAG
from airflow.models import XCom
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.db import provide_session
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from db_resources import (execute_clickhouse, execute_mssql, get_postgres_connection_string, execute_pg_hr)

# _log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

# stream_handler = logging.StreamHandler()
# stream_handler.setLevel(logging.INFO)
# stream_handler.setFormatter(logging.Formatter(_log_format))

# logger.addHandler(stream_handler)
logger = logging.getLogger('airflow.task')
default_args = {
    'owner': '',
    'start_date': datetime(year=2022,month=1,day=23,hour=0, minute=0),
    'email': ['.ru', '.ru'],
    'email_on_failure': True
}

# @provide_session
# def cleanup_xcom(context, session=None):
#     '''
#         Очистка xcom'ов из базы airflow
#     '''

#     dag_id = context["ti"]["dag_id"]
#     session.query(XCom).filter(XCom.dag_id == dag_id).delete()

dag = DAG(
    'update_table_checkerboard',
    schedule_interval='0 * * * *', # @hourly
    default_args=default_args,
    catchup=False,
    description='Обновление таблицы с шахматкой зарплаты',
    # on_success_callback=cleanup_xcom,
    max_active_runs=1,
)

def getPosLvlSalary():
    """
    Город	КодГорода	Должность	Уровень	Зарплата	Период	ДатаОбновления
    """
    query = f"""                                                                                                                                                                               
    WITH first_query AS
        (
        SELECT  
        --		reg_table.Должность AS Должность
                ШтатноеРасписание.Наименование AS Должность
                ,reg_table.Уровень AS Уровень
                ,dwh.[Справочник.Города].Наименование AS Город
                ,dwh.[Справочник.Города].Код AS КодГорода
                ,reg_table.Значение AS Зарплата
                , MAX(Период) OVER (PARTITION BY reg_table.Город, reg_table.Должность, reg_table.Уровень) AS max_period
                ,reg_table.Период as Период
                ,ШтатноеРасписание.ЛидерскийУровень  
        FROM dwh.[РегистрСведений.УровеньЗаработнойПлаты] AS reg_table
        JOIN dwh.[Справочник.Города] ON reg_table.Город = dwh.[Справочник.Города].Ссылка 
        JOIN dwh.[Справочник.ШтатноеРасписание] ШтатноеРасписание ON reg_table.Должность  =  ШтатноеРасписание.Ссылка  
            AND ШтатноеРасписание.ПрефиксКДолжности NOT IN  ('ДОМ', 'ГРП')
        )

        SELECT Город,КодГорода, Должность, Уровень, Зарплата, Период, max_period, CONVERT(DATE, GETDATE()) AS ДатаОбновления, ЛидерскийУровень
        FROM  first_query
        WHERE Период = max_period
            AND Должность != '2.03. Фед.развитие сайта'
            AND Должность not like '%(на аутсорсе)%' 
        ORDER BY Должность, Уровень
	 """
    df_GetPosLvlSalary = execute_mssql(query, '1c_replica')
    return df_GetPosLvlSalary

def getActualStaff():
    """
    ДатаОбновления	Должность	УровеньОплаты	Город	ТекущийФилиал	КоличествоСотрудников
    """
    query = f"""
  select
      as2."ДатаОбновления" 
    , as2."Должность"
    , as2."УровеньОплаты"
    , replace(as2."Город", 'РКЦ ', '') as "Город"
    , as2."ТекущийФилиал"
    ,count(*) as КоличествоСотрудников
  from actual_staff as2
  where as2."Должность" not like '%%(на аутсорсе)%%'
  group by as2."Должность", as2."РасчетнаяЗарплата", as2."УровеньОплаты", as2."ТекущийФилиал", as2."ДатаОбновления", as2."Город" 
  order by as2."Должность", as2."ТекущийФилиал", as2."УровеньОплаты" """
    df_getActualStaff = execute_pg_hr(query)
    return df_getActualStaff

def getBranchCity():
    """
    Код	Филиал
    """
    query = f"""
	select 
	  сф.Код 
	, сф.Наименование AS Филиал
from dwh.[Справочник.Фирмы] as сф
	"""
    df_branchCity = execute_mssql(query, '1c_replica')
    return df_branchCity

def getEmployeeCountOnBranch():
    """
    Дата	КодГорода	Наименование	Фирма	Должность	КоличествоСтавок	МаксДата
    """
    query = f""" 
    WITH МаксДатаЦТЕ AS (
    select 
    --	DATEFROMPARTS(YEAR(Дата), MONTH(Дата), 1) AS Дата
            Дата
        , сг.Код AS КодГорода 
        , сг.Наименование 
        , сф.Код as Фирма
        , сшр.Наименование as Должность
        , душрс.КоличествоСтавок
        , max(Дата) OVER (PARTITION BY сф.Код, сшр.Наименование) AS МаксДата
    from dwh.[Документ.УтверждениеШтатногоРасписания] душр
    join (
        select Фирма, YEAR(Дата) as Год, MONTH(Дата) as Месяц, MAX(DAY(Дата)) as День
        from dwh.[Документ.УтверждениеШтатногоРасписания] душрa 
        group by Фирма, YEAR(Дата), MONTH(Дата)
        ) tmp
        on tmp.Год = YEAR(душр.Дата)
        and tmp.Месяц = MONTH(душр.Дата)
        and tmp.День = DAY(душр.Дата)
        and tmp.Фирма = душр.Фирма 
    join dwh.[Документ.УтверждениеШтатногоРасписания.Состав] душрс
        on душрс.Ссылка = душр.Ссылка
    join dwh.[Справочник.Фирмы] сф 
        on сф.Ссылка = душр.Фирма 
    join dwh.[Справочник.ШтатноеРасписание] сшр 
        on сшр.Ссылка = душрс.Должность 
    JOIN dwh.[Справочник.Города] сг 
        ON сф.Город = сг.Ссылка 
    where Дата >= '2021-01-01'
        and Проведен = 0x01
    --	    AND сф.Код = 1293
    --	order by душр.Дата
                                )
    SELECT * FROM МаксДатаЦТЕ
    WHERE МаксДата = Дата
    """
    df_getEmployeeCountOnBranch = execute_mssql(query, '1c_replica')
    return df_getEmployeeCountOnBranch

def uploadTable(**context):

#  df_mean = context['task_instance'].xcom_pull('update_actual_staff')
    df_PosLvlSalary = context['task_instance'].xcom_pull('getPosLvlSalary')
    df_actual_staff = context['task_instance'].xcom_pull('getActualStaff')
    df_employeeCountOnBranch = context['task_instance'].xcom_pull('getEmployeeCountOnBranch')
    df_branchCity = context['task_instance'].xcom_pull('getBranchCity')
    logging.info('1')
    df_checkerboard_salary = df_actual_staff \
        .merge(df_branchCity, left_on='ТекущийФилиал', right_on='Код') \
        .merge(df_PosLvlSalary, left_on=['Город', 'Должность', 'УровеньОплаты'], right_on=['Город', 'Должность', 'Уровень'], how='right') \
        # .merge(df_getEmployeeCountOnBranch, left_on=['КодГорода', 'Должность'], right_on=['КодГорода','Должность'])
    logging.info('2')
    df_checkerboard_salary['КоличествоСотрудников'].fillna(0, inplace=True)
    df_checkerboard_salary = df_checkerboard_salary[['Город', 'Филиал', 'Должность', 
                            'Уровень', 'Зарплата','КоличествоСотрудников']] 
    logging.info('3')
    conection_url = get_postgres_connection_string('mart_hr')
    logging.info('4')  
    from sqlalchemy import create_engine
    eng = create_engine(conection_url)
    logging.info('5')
    df_checkerboard_salary.to_sql('checkerboard_salary', eng, if_exists='replace', 
                    method='multi', chunksize=1024, index=False)
    logging.info('6')
    eng.dispose()
    logging.info('7')
    return 



with dag:

    _getPosLvlSalary = PythonOperator(
                task_id='getPosLvlSalary',
                python_callable=getPosLvlSalary,
            )
    
    _getActualStaff = PythonOperator(
                task_id='getActualStaff',
                python_callable=getActualStaff,
            )
    _getBranchCity = PythonOperator(
                task_id='getBranchCity',
                python_callable=getBranchCity,
            )

    _getEmployeeCountOnBranch = PythonOperator(
                task_id='getEmployeeCountOnBranch',
                python_callable=getEmployeeCountOnBranch,
            )
    _uploadTable = PythonOperator(
                task_id='uploadTable',
                python_callable=uploadTable,
            )    

    start_dummy_op = DummyOperator(
        task_id='start'
    )

    end_dummy_op = DummyOperator(
        task_id = 'end'
    )

    start_dummy_op >> _getPosLvlSalary >> _getActualStaff  >> _getBranchCity >> _uploadTable >> end_dummy_op 
     # >> _getEmployeeCountOnBranch