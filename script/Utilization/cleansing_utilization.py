from sqlalchemy import create_engine
import pandas as pd
import re
import pickle
from datetime import datetime,timedelta
import sys
import timeit
import numpy as np
import pandas.io.common
from pytz import all_timezones

def konvertToDS(row):
    waktu = str(row)
    waktu = waktu[0:10]
    return waktu
def cleansing():
    print("LOADING CSV DATA ehealth")
    dbschema='sqminternet_rcalogic'
#     sqlEngine= create_engine('postgres://postgres:SQM_L4NJUT@10.62.169.52:5434/nossasqm', pool_recycle=3600,connect_args={'options': '-csearch_path={}'.format(dbschema)})
    sqlEngine= create_engine('postgres://postgres:SQM_b4r0k4h@10.62.169.52:5433/nossasqm', pool_recycle=3600,connect_args={'options': '-csearch_path={}'.format(dbschema)})
    try:
        col_names = ["readtimestamp","itemid","intmfname","metric","value","mfdisplayname","device","componentname"]
        ehealth_raw = pd.read_csv('/home/sqm_model/airflow/dags/script/Utilization/data/raw_ehealth_clean.csv',header=None, names=col_names, error_bad_lines=False,sep=';')
    except pandas.io.common.EmptyDataError:
        ehealth_raw = pd.DataFrame()
    if (ehealth_raw.empty):
        print("data csv ehealth tidak ada !")
    else:
        print(all_timezones)
        ehealth_raw['readtimestamp'] = pd.to_datetime(ehealth_raw['readtimestamp'],errors='coerce',unit='ms')
        ehealth_raw_source = pd.DataFrame()
        ehealth_raw_source['readtimestamp'] = ehealth_raw['readtimestamp']
        ehealth_raw_source['metric'] = ehealth_raw['metric']
        ehealth_raw_source['value'] = ehealth_raw['value']
        ehealth_raw_source['device'] = ehealth_raw['device']
        ehealth_raw_source['componentname'] = ehealth_raw['componentname']
        ehealth_raw_source = ehealth_raw_source[ehealth_raw_source.value <= 100]
        print("LOADING CSV EHEALTH SUCCESSFULLY")
        
        print("LOADING CSV INVENTORY")
        print(datetime.now())
        inventory_metro_access = pd.read_csv('/home/sqm_model/airflow/dags/script/Inventory_current_all/data/inventory_metro_access.csv',error_bad_lines=False)
        inventory_metro_bras = pd.read_csv('/home/sqm_model/airflow/dags/script/Inventory_current_all/data/inventory_metro_bras.csv',error_bad_lines=False)
        inventory_bras = pd.read_csv('/home/sqm_model/airflow/dags/script/Inventory_current_all/data/inventory_bras.csv',error_bad_lines=False)
        inventory_pe_speedy = pd.read_csv('/home/sqm_model/airflow/dags/script/Inventory_current_all/data/inventory_pe_speedy.csv',error_bad_lines=False)
        inventory_redirector = pd.read_csv('/home/sqm_model/airflow/dags/script/Inventory_current_all/data/inventory_redirector.csv',error_bad_lines=False)
        print("LOADING CSV INVENTORY SUCCESSFULLY")
        
        print("PREPROCESSING")
        print(datetime.now())
        dt_string = datetime.now().strftime("%Y_%m_%d_%H_%M")
        print("date and time =", dt_string)
        
        ehealth_me_access = pd.merge(ehealth_raw_source, inventory_metro_access,how='inner', left_on=['device','componentname'], right_on = ['metro_access','metro_access_port'])
        ehealth_me_access['readtimestamp']=ehealth_me_access.readtimestamp.dt.tz_localize('UTC').dt.tz_convert('Asia/Jakarta')
        ehealth_me_access_filter = pd.DataFrame()
        ehealth_me_access_filter['readtimestamp'] = ehealth_me_access['readtimestamp']
        ehealth_me_access_filter['metric'] = ehealth_me_access['metric']
        ehealth_me_access_filter['value'] = ehealth_me_access['value']
        ehealth_me_access_filter['metro_access'] = ehealth_me_access['metro_access']
        ehealth_me_access_filter['metro_access_port'] = ehealth_me_access['metro_access_port']
        ehealth_me_access_clean = ehealth_me_access_filter.groupby(['metro_access','metro_access_port','metric']).max().reset_index()
        ehealth_me_access_clean = ehealth_me_access_clean.pivot_table(
        values='value', 
        index=['metro_access', 'metro_access_port','readtimestamp'], 
        columns='metric', 
        aggfunc=np.max)
        ehealth_me_access_clean = ehealth_me_access_clean.reset_index()
        ehealth_me_access_fix = pd.DataFrame()
        ehealth_me_access_fix['me_access_hostname'] = ehealth_me_access_clean['metro_access']
        ehealth_me_access_fix['me_access_port'] = ehealth_me_access_clean['metro_access_port']
        ehealth_me_access_fix['me_access_utilization_value'] = ehealth_me_access_clean['Utilization']
        ehealth_me_access_fix['me_access_utilizationIn_value'] = ehealth_me_access_clean['UtilizationIn']
        ehealth_me_access_fix['me_access_utilizationOut_value'] = ehealth_me_access_clean['UtilizationOut']
        ehealth_me_access_fix['me_access_utilization_update'] = ehealth_me_access_clean['readtimestamp']
        ehealth_me_access_fix
#        dbConnection= sqlEngine.connect()
        #     dbConnection.execute("TRUNCATE TABLE sqminternet_rcalogic.sqmrca_ne_access_temperature")
#        ehealth_me_access_fix.to_sql('sqmrca_ehealth_me_access_source', con = sqlEngine, if_exists = 'replace', index=None,schema='sqminternet_rcalogic')
#        dbConnection.execute("""
#        update sqminternet_rcalogic.sqmrca_ne_status_treshold a
#            set me_access_utilization_value = b.me_access_utilization_value, me_access_utilization_update =b.me_access_utilization_update
#            from 
#            (
#            select * from 
#              (  
#                select me_access_hostname,me_access_port,me_access_utilization_update,max(me_access_utilization_value) me_access_utilization_value
#                from sqminternet_rcalogic.sqmrca_ehealth_me_access_source
#                where( me_access_hostname,me_access_port,me_access_utilization_update) in
#                (
#                  select me_access_hostname,me_access_port,max(me_access_utilization_update)
#                  from sqminternet_rcalogic.sqmrca_ehealth_me_access_source
#                  group by me_access_hostname,me_access_port
#                ) group by me_access_hostname,me_access_port,me_access_utilization_update
#              )x
#            )b
#            where a.me_access_hostname=b.me_access_hostname and a.me_access_port = b.me_access_port
#        """)
#        dbConnection.close()
        ehealth_me_access_clean_fix_result = pd.DataFrame()
        ehealth_me_access_clean_fix_result['hostname'] = ehealth_me_access_clean['metro_access']
        ehealth_me_access_clean_fix_result['port'] = ehealth_me_access_clean['metro_access_port']
        ehealth_me_access_clean_fix_result['utilization_value'] = ehealth_me_access_clean['Utilization']
        ehealth_me_access_clean_fix_result['utilizationIn_value'] = ehealth_me_access_clean['UtilizationIn']
        ehealth_me_access_clean_fix_result['utilizationOut_value'] = ehealth_me_access_clean['UtilizationOut']
        ehealth_me_access_clean_fix_result['utilization_update'] = ehealth_me_access_clean['readtimestamp']
        ehealth_me_access_clean_fix_result['node_type'] = 'me-access'
        ehealth_me_access_clean_fix_result
        
        
        
        
        ehealth_me_bras = pd.merge(ehealth_raw_source, inventory_metro_bras,how='inner', left_on=['device','componentname'], right_on = ['metro_bras','metro_bras_port'])
        ehealth_me_bras['readtimestamp']=ehealth_me_bras.readtimestamp.dt.tz_localize('UTC').dt.tz_convert('Asia/Jakarta')
#         ehealth_me_bras['readtimestamp'] = pd.DatetimeIndex(ehealth_me_bras['readtimestamp']) + timedelta(hours=7)
        ehealth_me_bras_filter = pd.DataFrame()
        ehealth_me_bras_filter['readtimestamp'] = ehealth_me_bras['readtimestamp']
        ehealth_me_bras_filter['metric'] = ehealth_me_bras['metric']
        ehealth_me_bras_filter['value'] = ehealth_me_bras['value']
        ehealth_me_bras_filter['metro_bras'] = ehealth_me_bras['metro_bras']
        ehealth_me_bras_filter['metro_bras_port'] = ehealth_me_bras['metro_bras_port']
        
        ehealth_me_bras_clean = ehealth_me_bras_filter.groupby(['metro_bras','metro_bras_port','metric']).max().reset_index()
        ehealth_me_bras_clean = ehealth_me_bras_clean.pivot_table(
                values='value', 
                index=['metro_bras', 'metro_bras_port','readtimestamp'], 
                columns='metric', 
                aggfunc=np.max)
        ehealth_me_bras_clean = ehealth_me_bras_clean.reset_index()
        ehealth_me_bras_clean_fix = pd.DataFrame()
        ehealth_me_bras_clean_fix['me_bras_hostname'] = ehealth_me_bras_clean['metro_bras']
        ehealth_me_bras_clean_fix['me_bras_port'] = ehealth_me_bras_clean['metro_bras_port']
        ehealth_me_bras_clean_fix['me_bras_utilization_value'] = ehealth_me_bras_clean['Utilization']
        ehealth_me_bras_clean_fix['me_bras_utilizationIn_value'] = ehealth_me_bras_clean['UtilizationIn']
        ehealth_me_bras_clean_fix['me_bras_utilizationOut_value'] = ehealth_me_bras_clean['UtilizationOut']
        ehealth_me_bras_clean_fix['me_bras_utilization_update'] = ehealth_me_bras_clean['readtimestamp']
        
        ehealth_me_bras_clean_fix

	#dbConnection= sqlEngine.connect()
        #ehealth_me_bras_clean_fix.to_sql('sqmrca_ehealth_me_bras_source', con = sqlEngine, if_exists = 'replace', index=None,schema='sqminternet_rcalogic')
        #dbConnection.execute("""
        #update sqminternet_rcalogic.sqmrca_ne_status_treshold a
        #    set me_bras_utilization_value = b.me_bras_utilization_value, me_bras_utilization_update =b.me_bras_utilization_update
        #    from 
        #    (
        #    select * from 
        #      (  
        #        select me_bras_hostname,me_bras_port,me_bras_utilization_update,max(me_bras_utilization_value) me_bras_utilization_value
        #        from sqminternet_rcalogic.sqmrca_ehealth_me_bras_source
        #        where( me_bras_hostname,me_bras_port,me_bras_utilization_update) in
        #        (
        #          select me_bras_hostname,me_bras_port,max(me_bras_utilization_update)
        #          from sqminternet_rcalogic.sqmrca_ehealth_me_bras_source
        #          group by me_bras_hostname,me_bras_port
        #       ) group by me_bras_hostname,me_bras_port,me_bras_utilization_update
        #      )x
        #    )b
        #    where a.me_bras_hostname=b.me_bras_hostname and a.me_bras_port = b.me_bras_port
        #""")
        #dbConnection.close()
        
        ehealth_me_bras_clean_fix_result = pd.DataFrame()
        ehealth_me_bras_clean_fix_result['hostname'] = ehealth_me_bras_clean['metro_bras']
        ehealth_me_bras_clean_fix_result['port'] = ehealth_me_bras_clean['metro_bras_port']
        ehealth_me_bras_clean_fix_result['utilization_value'] = ehealth_me_bras_clean['Utilization']
        ehealth_me_bras_clean_fix_result['utilizationIn_value'] = ehealth_me_bras_clean['UtilizationIn']
        ehealth_me_bras_clean_fix_result['utilizationOut_value'] = ehealth_me_bras_clean['UtilizationOut']
        ehealth_me_bras_clean_fix_result['utilization_update'] = ehealth_me_bras_clean['readtimestamp']
        ehealth_me_bras_clean_fix_result['node_type'] = 'me-bras'
        ehealth_me_bras_clean_fix
        
        
        ehealth_bras = pd.merge(ehealth_raw_source, inventory_bras,how='inner', left_on=['device','componentname'], right_on = ['bras','int_bras'])
        ehealth_bras['readtimestamp']=ehealth_bras.readtimestamp.dt.tz_localize('UTC').dt.tz_convert('Asia/Jakarta')
#         ehealth_bras['readtimestamp'] = pd.DatetimeIndex(ehealth_bras['readtimestamp']) + timedelta(hours=7)
        ehealth_bras_filter = pd.DataFrame()
        ehealth_bras_filter['readtimestamp'] = ehealth_bras['readtimestamp']
        ehealth_bras_filter['metric'] = ehealth_bras['metric']
        ehealth_bras_filter['value'] = ehealth_bras['value']
        ehealth_bras_filter['bras'] = ehealth_bras['bras']
        ehealth_bras_filter['int_bras'] = ehealth_bras['int_bras']
        ehealth_bras_clean = ehealth_bras_filter.groupby(['bras','int_bras','metric']).max().reset_index()
        ehealth_bras_clean = ehealth_bras_clean.pivot_table(
        values='value', 
        index=['bras', 'int_bras','readtimestamp'], 
        columns='metric', 
        aggfunc=np.max)
        ehealth_bras_clean = ehealth_bras_clean.reset_index()
        ehealth_bras_clean_fix = pd.DataFrame()
        ehealth_bras_clean_fix['bras_hostname'] = ehealth_bras_clean['bras']
        ehealth_bras_clean_fix['bras_port'] = ehealth_bras_clean['int_bras']
        ehealth_bras_clean_fix['bras_utilization_value'] = ehealth_bras_clean['Utilization']
        ehealth_bras_clean_fix['bras_utilizationIn_value'] = ehealth_bras_clean['UtilizationIn']
        ehealth_bras_clean_fix['bras_utilizationOut_value'] = ehealth_bras_clean['UtilizationOut']
        ehealth_bras_clean_fix['bras_utilization_update'] = ehealth_bras_clean['readtimestamp']
        ehealth_bras_clean_fix
        #dbConnection= sqlEngine.connect()
        #     dbConnection.execute("TRUNCATE TABLE sqminternet_rcalogic.sqmrca_ne_access_temperature")
        #ehealth_bras_clean_fix.to_sql('sqmrca_ehealth_bras_source', con = sqlEngine, if_exists = 'replace', index=None,schema='sqminternet_rcalogic')
        #dbConnection.execute("""
        #update sqminternet_rcalogic.sqmrca_ne_status_treshold a
        #    set bras_utilization_value = b.bras_utilization_value, bras_utilization_update =b.bras_utilization_update
        #    from 
        #    (
        #    select * from 
        #      (  
        #        select bras_hostname,bras_port,bras_utilization_update,max(bras_utilization_value) bras_utilization_value
        #        from sqminternet_rcalogic.sqmrca_ehealth_bras_source
        #        where( bras_hostname,bras_port,bras_utilization_update) in
        #        (
        #          select bras_hostname,bras_port,max(bras_utilization_update)
        #          from sqminternet_rcalogic.sqmrca_ehealth_bras_source
        #          group by bras_hostname,bras_port
        #        ) group by bras_hostname,bras_port,bras_utilization_update
        #      )x
        #    )b
        #    where a.bras_hostname=b.bras_hostname and a.bras_port = b.bras_port
        #""")
        #dbConnection.close()
        ehealth_bras_clean_fix_result = pd.DataFrame()
        ehealth_bras_clean_fix_result['hostname'] = ehealth_bras_clean['bras']
        ehealth_bras_clean_fix_result['port'] = ehealth_bras_clean['int_bras']
        ehealth_bras_clean_fix_result['utilization_value'] = ehealth_bras_clean['Utilization']
        ehealth_bras_clean_fix_result['utilizationIn_value'] = ehealth_bras_clean['UtilizationIn']
        ehealth_bras_clean_fix_result['utilizationOut_value'] = ehealth_bras_clean['UtilizationOut']
        ehealth_bras_clean_fix_result['utilization_update'] = ehealth_bras_clean['readtimestamp']
        ehealth_bras_clean_fix_result['node_type'] = 'bras'
        
        
        
        ehealth_pe_speedy = pd.merge(ehealth_raw_source, inventory_pe_speedy,how='inner', left_on=['device'], right_on = ['pe_gw_pe_speedy'])        
        ehealth_pe_speedy['readtimestamp'] = ehealth_pe_speedy.readtimestamp.dt.tz_localize('UTC').dt.tz_convert('Asia/Jakarta')
#         ehealth_pe_speedy['readtimestamp'] = pd.DatetimeIndex(ehealth_pe_speedy['readtimestamp']) + timedelta(hours=7)
        ehealth_pe_speedy_filter = pd.DataFrame()
        ehealth_pe_speedy_filter['readtimestamp'] = ehealth_pe_speedy['readtimestamp']
        ehealth_pe_speedy_filter['metric'] = ehealth_pe_speedy['metric']
        ehealth_pe_speedy_filter['value'] = ehealth_pe_speedy['value']
        ehealth_pe_speedy_filter['pe_gw_pe_speedy'] = ehealth_pe_speedy['pe_gw_pe_speedy']
        
        ehealth_pe_speedy_clean = ehealth_pe_speedy_filter.groupby(['pe_gw_pe_speedy','metric']).max().reset_index()
        ehealth_pe_speedy_clean = ehealth_pe_speedy_clean.pivot_table(
        values='value', 
        index=['pe_gw_pe_speedy','readtimestamp'], 
        columns='metric', 
        aggfunc=np.max)
        ehealth_pe_speedy_clean = ehealth_pe_speedy_clean.reset_index()
     
        ehealth_pe_speedy_clean_fix = pd.DataFrame()
        ehealth_pe_speedy_clean_fix['pe_inet_hostname'] = ehealth_pe_speedy_clean['pe_gw_pe_speedy']
        ehealth_pe_speedy_clean_fix['pe_inet_utilization_value'] = ehealth_pe_speedy_clean['Utilization']
        ehealth_pe_speedy_clean_fix['pe_inet_utilizationIn_value'] = ehealth_pe_speedy_clean['UtilizationIn']
        ehealth_pe_speedy_clean_fix['pe_inet_utilizationOut_value'] = ehealth_pe_speedy_clean['UtilizationOut']
        ehealth_pe_speedy_clean_fix['pe_inet_utilization_update'] = ehealth_pe_speedy_clean['readtimestamp']
        
        ehealth_pe_speedy_clean_fix
        #dbConnection= sqlEngine.connect()
        #     dbConnection.execute("TRUNCATE TABLE sqminternet_rcalogic.sqmrca_ne_access_temperature")
        ehealth_pe_speedy_clean_fix.to_sql('sqmrca_ehealth_pe_inet_source', con = sqlEngine, if_exists = 'replace', index=None,schema='sqminternet_rcalogic')
        #dbConnection.execute("""
        #update sqminternet_rcalogic.sqmrca_ne_status_treshold a
        #    set pe_inet_utilization_value = b.pe_inet_utilization_value, pe_inet_utilization_update =b.pe_inet_utilization_update
        #    from 
        #    (
        #    select * from 
        #      (  
        #        select pe_inet_hostname,pe_inet_utilization_update,max(pe_inet_utilization_value) pe_inet_utilization_value
        #        from sqminternet_rcalogic.sqmrca_ehealth_pe_inet_source
        #        where( pe_inet_hostname,pe_inet_utilization_update) in
        #        (
        #          select pe_inet_hostname,max(pe_inet_utilization_update)
        #          from sqminternet_rcalogic.sqmrca_ehealth_pe_inet_source
        #          group by pe_inet_hostname
        #        ) group by pe_inet_hostname,pe_inet_utilization_update
        #      )x
        #    )b
        #    where a.pe_inet_hostname=b.pe_inet_hostname;
        #""")
        #dbConnection.close()
        ehealth_pe_speedy_clean_fix_result = pd.DataFrame()
        ehealth_pe_speedy_clean_fix_result['hostname'] = ehealth_pe_speedy_clean['pe_gw_pe_speedy']
        ehealth_pe_speedy_clean_fix_result['utilization_value'] = ehealth_pe_speedy_clean['Utilization']
        ehealth_pe_speedy_clean_fix_result['utilizationIn_value'] = ehealth_pe_speedy_clean['UtilizationIn']
        ehealth_pe_speedy_clean_fix_result['utilizationOut_value'] = ehealth_pe_speedy_clean['UtilizationOut']
        ehealth_pe_speedy_clean_fix_result['utilization_update'] = ehealth_pe_speedy_clean['readtimestamp']
        ehealth_pe_speedy_clean_fix_result['node_type'] = 'pe-speedy'
        
        
        
        ehealth_redirector = pd.merge(ehealth_raw_source, inventory_redirector,how='inner', left_on=['device'], right_on = ['pe_gw_redirector'])
#         ehealth_redirector['readtimestamp'] = pd.DatetimeIndex(ehealth_redirector['readtimestamp']) + timedelta(hours=7)
        ehealth_redirector_filter = pd.DataFrame()
        ehealth_redirector_filter['readtimestamp'] = ehealth_redirector['readtimestamp']
        ehealth_redirector_filter['metric'] = ehealth_redirector['metric']
        ehealth_redirector_filter['value'] = ehealth_redirector['value']
        ehealth_redirector_filter['pe_gw_redirector'] = ehealth_redirector['pe_gw_redirector']
        
        ehealth_redirector_clean = ehealth_redirector_filter.groupby(['pe_gw_redirector','metric']).max().reset_index()
        ehealth_redirector_clean = ehealth_redirector_clean.pivot_table(
                values='value', 
                index=['pe_gw_redirector','readtimestamp'], 
                columns='metric', 
                aggfunc=np.max)
        ehealth_redirector_clean = ehealth_redirector_clean.reset_index()
        ehealth_redirector_clean_fix = pd.DataFrame()
        ehealth_redirector_clean_fix['redirector_hostname'] = ehealth_redirector_clean['pe_gw_redirector']
        ehealth_redirector_clean_fix['redirector_utilization_value'] = ehealth_redirector_clean['Utilization']
        ehealth_redirector_clean_fix['redirector_utilizationIn_value'] = ehealth_redirector_clean['UtilizationIn']
        ehealth_redirector_clean_fix['redirector_utilizationOut_value'] = ehealth_redirector_clean['UtilizationOut']
        ehealth_redirector_clean_fix['redirector_utilization_update'] = ehealth_redirector_clean['readtimestamp']
        ehealth_redirector_clean_fix

        #dbConnection= sqlEngine.connect()
        #     dbConnection.execute("TRUNCATE TABLE sqminternet_rcalogic.sqmrca_ne_access_temperature")
        #ehealth_redirector_clean_fix.to_sql('sqmrca_ehealth_redirector_source', con = sqlEngine, if_exists = 'replace', index=None,schema='sqminternet_rcalogic')
        #dbConnection.execute("""
        #update sqminternet_rcalogic.sqmrca_ne_status_treshold a
        #    set redirector_utilization_value = b.redirector_utilization_value, redirector_utilization_update =b.redirector_utilization_update
        #    from 
        #    (
        #    select * from 
        #      (  
        #        select redirector_hostname,redirector_utilization_update,max(redirector_utilization_value) redirector_utilization_value
        #        from sqminternet_rcalogic.sqmrca_ehealth_redirector_source
        #        where( redirector_hostname,redirector_utilization_update) in
        #        (
        #          select redirector_hostname,max(redirector_utilization_update)
        #          from sqminternet_rcalogic.sqmrca_ehealth_redirector_source
        #          group by redirector_hostname
        #        ) group by redirector_hostname,redirector_utilization_update
        #      )x
        #    )b
        #    where a.redirector_hostname=b.redirector_hostname
        #""")
        #dbConnection.close()
        ehealth_redirector_clean_fix_result = pd.DataFrame()
        ehealth_redirector_clean_fix_result['hostname'] = ehealth_redirector_clean['pe_gw_redirector']
        ehealth_redirector_clean_fix_result['utilization_value'] = ehealth_redirector_clean['Utilization']
        ehealth_redirector_clean_fix_result['utilizationIn_value'] = ehealth_redirector_clean['UtilizationIn']
        ehealth_redirector_clean_fix_result['utilizationOut_value'] = ehealth_redirector_clean['UtilizationOut']
        ehealth_redirector_clean_fix_result['utilization_update'] = ehealth_redirector_clean['readtimestamp']
        ehealth_redirector_clean_fix_result['node_type'] = 'redirector'
        
        
        
        df_utilization = pd.DataFrame()
        df_utilization = pd.concat([ehealth_me_access_clean_fix_result, ehealth_me_bras_clean_fix_result], sort=False)
        df_utilization = pd.concat([df_utilization, ehealth_bras_clean_fix_result], sort=False)
        df_utilization = pd.concat([df_utilization, ehealth_pe_speedy_clean_fix_result], sort=False)
        df_utilization = pd.concat([df_utilization, ehealth_redirector_clean_fix_result], sort=False)
#         df_utilization['utilization_update'] = df_utilization['utilization_update'].to_string()
        df_utilization['ds'] = df_utilization.apply(lambda row : konvertToDS(row['utilization_update']), axis=1)
        df_utilization.to_csv('/home/sqm_model/airflow/dags/script/Utilization/data/utilization_' + dt_string +'.csv',index=None)

