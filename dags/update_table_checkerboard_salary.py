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
    'start_date': datetime(year=2022,month=1,day=23,hour=0, minute=0),
    'owner': '',
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
    schedule_interval='0 * * * *', # @hourчёly
    default_args=default_args,
    catchup=False,
    description='Обновление таблицы с шахматкой зарплаты',
    # on_success_callback=cleanup_xcom,
    max_active_runs=1,
)

def getPosLvlSalary():
    query = f"""                                                                                                                                                                               
	  WITH first_query AS
        (
SELECT  
--		reg_table.Должность AS Должность
        ШтатноеРасписание.Наименование AS Должность
        ,рсузп.Уровень AS Уровень
        ,сг.Наименование AS Город
        ,сг.Код AS КодГорода
        , сг.Регион AS СсылкаРегион
        ,рсузп.Значение AS Зарплата
        , MAX(Период) OVER (PARTITION BY рсузп.Город, рсузп.Должность, рсузп.Уровень) AS max_period
        ,рсузп.Период as Период
        ,ШтатноеРасписание.ЛидерскийУровень
        , рсузп.Город  AS СсылкаГород
        , сг.ТерриториальноеРасположение AS СсылкаГородТерритория
FROM dwh.[РегистрСведений.УровеньЗаработнойПлаты] AS рсузп
JOIN dwh.[Справочник.Города] AS сг 
	ON рсузп.Город = сг.Ссылка 
JOIN dwh.[Справочник.ШтатноеРасписание] ШтатноеРасписание ON рсузп.Должность  =  ШтатноеРасписание.Ссылка  
    AND ШтатноеРасписание.ПрефиксКДолжности NOT IN  ('ДОМ', 'ГРП')      		
        ),
findMaxDate AS (
SELECT 
	  Город
	, КодГорода
	, Должность
	, Уровень
	, Зарплата
	, Период
	, max_period
	, CONVERT(DATE, GETDATE()) AS ДатаОбновления
	, ЛидерскийУровень
	, СсылкаГород
	, СсылкаРегион
	
FROM  first_query
WHERE Период = max_period
    AND Должность != '2.03. Фед.развитие сайта'
    AND Должность not like '%(на аутсорсе)%' 
        ),
joinTerrerory AS (
	SELECT
		 ст.Наименование AS Территория
		, сг.Ссылка AS СсылкаГорода
	FROM dwh.[Справочник.Города] сг
	JOIN dwh.[Справочник.Фирмы] сф
		ON сф.Город = сг.Ссылка
	JOIN dwh.[Справочник.Фирмы.ИерархияТерриторий] сфит
		ON сф.Ссылка = сфит.Ссылка
		AND сфит.Уровень = 3
	JOIN dwh.[Справочник.Территории] ст
		ON ст.Ссылка = сфит.Территория
	GROUP BY
		 ст.Наименование
		, сг.Ссылка 	
		)	
SELECT
	  Территория
	, Город
	, КодГорода
	, Должность
	, findMaxDate.Уровень
	, Зарплата
	, CONVERT(DATE, GETDATE()) AS ДатаОбновления
	, ЛидерскийУровень
FROM findMaxDate
JOIN joinTerrerory
	ON СсылкаГород = joinTerrerory.СсылкаГорода
			                                         """
    df_GetPosLvlSalary = execute_mssql(query, '1c_replica')
    return df_GetPosLvlSalary

def getActualStaff():
    """
    ДатаОбновления	Должность	УровеньОплаты	Город	ТекущийФилиал	КоличествоСотрудников
    """
    query = f"""
 select
     as2."Должность"
    , as2."УровеньОплаты"
    , as2."ТекущийФилиал"
    ,count(*) as КоличествоСотрудников
    , as2."Город"
    , as2."Филиал"
  from actual_staff as2
  where as2."Должность" not like '%%(на аутсорсе)%%'
  group by as2."Должность", as2."УровеньОплаты", as2."ТекущийФилиал", as2."ДатаОбновления", as2."Город", as2."Филиал", as2."РРС" 
  order by as2."Должность", as2."ТекущийФилиал", as2."УровеньОплаты" 
   """
    df_getActualStaff = execute_pg_hr(query)
    return df_getActualStaff


def getLossBranches():

	"""НазваниеФирмы	НазваниеГорода	Территория	Должность	Уровень"""

	query = f"""
	
WITH МаксДатаЦТЕ AS (
	select 
	--	DATEFROMPARTS(YEAR(Дата), MONTH(Дата), 1) AS Дата
			Дата
		, сф.Наименование AS НазваниеФирмы 
		, сг.Код AS КодГорода 
		, сг.Наименование as НазваниеГорода
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
	), result_table AS
	(
	SELECT НазваниеФирмы, НазваниеГорода, Должность
	FROM МаксДатаЦТЕ
	WHERE МаксДата = Дата
	group by НазваниеФирмы, НазваниеГорода, Должность
	)
	select rt1.*
		, 1 as Уровень
	from result_table rt1
	union all 
	select rt2.*
		, 2 as Уровень
	from result_table rt2
	union all
	select rt3.*
		, 3 as Уровень
	from result_table rt3			
	"""

	df = execute_mssql(query, '1c_replica')
	return df

def uploadTable(**context):

#  df_mean = context['task_instance'].xcom_pull('update_actual_staff')
    df_PosLvlSalary = context['task_instance'].xcom_pull('getPosLvlSalary')
    df_actual_staff = context['task_instance'].xcom_pull('getActualStaff')
    df_loss_branches = context['task_instance'].xcom_pull('getLossBranches')
    logging.info('1')
    df_main =  df_loss_branches\
        .merge(df_actual_staff, left_on=['НазваниеФирмы', 'Должность', 'Уровень'], right_on=['Филиал', 'Должность', 'УровеньОплаты'], how='left')
    df_main['Город'] = df_main['Город'].fillna(df_main['НазваниеГорода'])
    df_main['Филиал'] = df_main['Филиал'].fillna(df_main['НазваниеФирмы'])
    
    df_checkerboard_salary = df_PosLvlSalary.merge(df_main, left_on=['Город','Должность', 'Уровень'], right_on=['Город', 'Должность', 'Уровень'], how='left')
    df_checkerboard_salary = df_checkerboard_salary[['Территория','Город', 'Филиал', 'Должность', 
                                'Уровень', 'Зарплата','ЛидерскийУровень','КоличествоСотрудников']]

    df_checkerboard_salary['КоличествоСотрудников'] = df_checkerboard_salary['КоличествоСотрудников'].fillna(0)

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

    _getLossBranches = PythonOperator(
                task_id='getLossBranches',
                python_callable=getLossBranches,
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

    start_dummy_op >> _getPosLvlSalary >> _getActualStaff  >>  _getLossBranches >> _uploadTable >> end_dummy_op 
