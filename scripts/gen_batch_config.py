import argparse
import json
import os, sys
from db_execution import *

batch_root_dir='../ctl/batches'

def init_args():
    # Parse Arguments
    parser = argparse.ArgumentParser(description='List DB objects.')
    parser.add_argument('--cnx', dest='db_cnx_name', help='Enter DB Connection Name')
    parser.add_argument('--sl', dest='db_where_txt', help='Enter Schema List as Comma Separated String')
    parser.add_argument('--bc', dest='batch_config_file', help='Enter Batch Config File Name')
    parser.add_argument('--bn', dest='batch_nm', help='Enter Batch Name')
    args = parser.parse_args()
    return args

def gen_batch_config(db_cnx_name, db_where_txt, batch_config_file, batch_nm):  
    db_where=gen_db_where(db_where_txt)
    db_obj_sql=gen_db_obj_sql(db_cnx_name,db_where)
    db_cnx_info=gen_db_cnx_info(db_cnx_name=db_cnx_name)
    conn=db_connect(db_cnx_info)
    table_list=list(db_execute(conn=conn, db_sql=db_obj_sql))

    # Get Default Batch Config
    with open(batch_config_file,'r') as bc:
        v_batch_config=json.load(bc)
    v_batch_config.update({"source_cnx_nm":db_cnx_name})
    v_batch_config.update({"table_list":table_list})
    v_batch_config["batch"]["batch_nm"]=batch_nm
    v_batch_config["batch"]["batch_desc"]=batch_nm

    # Generate Batch Config
    final_batch_path=batch_root_dir + '/' + batch_nm
    final_batch_config=final_batch_path + '/batch_' + batch_nm + '.cfg'
    if not os.path.exists(final_batch_path):
        os.makedirs(final_batch_path)
    with open(final_batch_config,'w') as nbc:
        nbc.write(json.dumps(v_batch_config, indent=4))    
    return final_batch_path, v_batch_config
    
def gen_table_config(final_batch_path, batch_config):
    batch_nm=batch_config["batch"]["batch_nm"]
    batch_type=batch_config["batch"]["batch_type"]
    hdfs_tgt_dir=batch_config["target_hive"]["hdfs_tgt_dir"]     
    for table in batch_config["table_list"]:
        table_config=batch_config.copy()
        schema_nm, table_nm=table["table_nm"].split('.') 
        table_config["source_extract"]["db_schema_nm"]=schema_nm
        table_config["source_extract"]["db_table_nm"]=table_nm
        table_config["target_hive"]["hdfs_tgt_dir"]=hdfs_tgt_dir + '/' + batch_nm + '/' + schema_nm + '__' + table_nm
        table_config["target_hive"]["hive_table"]=schema_nm + '__' + table_nm
        sqoop_job_nm=batch_nm + "_" + schema_nm + "_" + table_nm
        table_config["job"]["sqoop_job_nm"]=sqoop_job_nm
        table_config["job"]["sqoop_job_type"]=batch_type
        del table_config["table_list"]
        table_config_file=final_batch_path + '/job_' + sqoop_job_nm + '.cfg'
        with open(table_config_file,'w') as tc:
            tc.write(json.dumps(table_config, indent=4))

#==Main================
#if __name__=="main":
if 1==1:
    args=init_args()
    db_cnx_name=args.db_cnx_name
    db_where_txt=args.db_where_txt
    batch_config_file=args.batch_config_file
    batch_nm=args.batch_nm
    final_batch_path, batch_config=gen_batch_config(db_cnx_name, db_where_txt, batch_config_file, batch_nm)
    gen_table_config(final_batch_path, batch_config)
    
