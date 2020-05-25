from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext

import argparse
import json
import os, sys

def init_args():
    parser = argparse.ArgumentParser(description='Parse Arguments')
    parser.add_argument('--ctl', dest='ctl_file', required=True, help='Enter Job Control File')
    args = parser.parse_args()
    ctl_file=args.ctl_file
    with open(ctl_file,'r') as ctl_file:
        ctl_params=json.load(ctl_file)
    return ctl_params 

def f_nvl2(input_str,null_val, not_null_val):
    if input_str==None or input_str=="":
        result=null_val
    else:
        result=not_null_val
    return result      

def get_cnx_list(source_cnx_nm):
    cnx_file="/home/hadoop/config/db_cnx.cfg"
    with open(cnx_file,'r') as cnx_file:
        cnx_list=json.load(cnx_file)
    source_cnx_info=next(cnx_item.values()[0] for cnx_item in cnx_list if cnx_item.keys()[0] == source_cnx_nm)    
    return source_cnx_info

def add_db_option(db_call_string, option_nm, option_val):
    if option_val==None or option_val=="":
        db_call_string=db_call_string
    else:
        db_call_string=db_call_string + '.option("' + option_nm + '","' + option_val + '")'
    return db_call_string

def get_src_df(source_cnx_info, ctl_params):
    db_jar=source_cnx_info.get("db_jar")
    db_jdbc_url=source_cnx_info.get("db_jdbc_url")
    db_driver=source_cnx_info.get("db_driver")
    db_user=source_cnx_info.get("db_properties").get("user")
    db_pass=source_cnx_info.get("db_properties").get("password")
    db_columns=ctl_params["source_extract"].get("db_columns","*")
    db_where_clause=ctl_params["source_extract"].get("db_where_clause","1=1")
    db_schema_nm=ctl_params["source_extract"].get("db_schema_nm")
    db_table_nm=ctl_params["source_extract"].get("db_table_nm")
    db_query="select "+ db_columns + " from " + db_schema_nm + "." + db_table_nm + " where " + db_where_clause
    opt_db_connect='spark.read.format("jdbc")'
    opt_db_connect=add_db_option(opt_db_connect, "url", db_jdbc_url)
    opt_db_connect=add_db_option(opt_db_connect, "driver", db_driver)
    opt_db_connect=add_db_option(opt_db_connect, "user", db_user)
    opt_db_connect=add_db_option(opt_db_connect, "password", db_pass)
    db_split_columns=ctl_params["parallelism"].get("db_split_columns","")
    if db_split_columns:
       db_split_query="select min(" + db_split_columns + ") lowerBound,  max("+ db_split_columns + ") upperBound from "+ db_schema_nm + "." + db_table_nm + " where " + db_where_clause 
       opt_db_split_exec=add_db_option(opt_db_connect, "query", db_split_query) + ".load()"  
       print(opt_db_split_exec)
       opt_db_split_df=eval(opt_db_split_exec)
       result=opt_db_split_df.select("lowerBound","upperBound").collect()
       lowerBound=str(result[0].lowerBound)
       upperBound=str(result[0].upperBound)
       opt_db_connect=add_db_option(opt_db_connect, "dbtable", "("+db_query+ ") as t")
       opt_db_connect=add_db_option(opt_db_connect, "partitionColumn", db_split_columns)
       opt_db_connect=add_db_option(opt_db_connect, "lowerBound", lowerBound)
       opt_db_connect=add_db_option(opt_db_connect, "upperBound", upperBound)
       
       num_mappers=ctl_params["parallelism"].get("num_mappers","")
       opt_db_connect=add_db_option(opt_db_connect, "numPartitions", str(num_mappers))
    else:
       opt_db_connect=add_db_option(opt_db_connect, "dbtable", "("+db_query+ ") as t")
       
    db_query_exec=opt_db_connect+'.load()'
    src_df=eval(db_query_exec)  
    return src_df

def get_write_df(src_df, ctl_params):  
    target_hdfs=ctl_params["target_hdfs"]        
    hdfs_tgt_dir=target_hdfs["hdfs_tgt_dir"]
    hdfs_load_type=target_hdfs.get("hdfs_load_type","overwrite")
    hdfs_format=target_hdfs.get("tgt_data_format_type","parquet")
    if hdfs_format=='text':
       txt_field_term=target_hdfs.get("txt_field_term","|")
       txt_field_term=f_nvl2(txt_field_term,"|",txt_field_term)
       txt_escape=target_hdfs.get("txt_escape","")
       txt_escape=f_nvl2(txt_escape,"\\",txt_escape)
       txt_enclose="\\\""
       v_hdfs_format="csv"       
    else:       
       txt_field_term=""
       txt_escape=""     
       txt_enclose=""
       v_hdfs_format=hdfs_format
    hdfs_compression=target_hdfs.get("compress_type","")
    tgt_partition_columns=target_hdfs.get("tgt_partition_columns","")
    tgt_partition_column_list=[]
    if tgt_partition_columns!="":
       v_part_col_str=''
       tgt_partition_column_list=tgt_partition_columns.split(',')
       for part_col in tgt_partition_column_list:
            v_part_col_str=f_nvl2(v_part_col_str, '', v_part_col_str + ',') + '"' + part_col + '"'
    else:
       v_part_col_str=""       
    opt_hdfs_options = 'src_df.write.format("' + v_hdfs_format + '")'
    opt_hdfs_options = add_db_option(opt_hdfs_options, "delimiter", txt_field_term)
    opt_hdfs_options = add_db_option(opt_hdfs_options, "escapeChar", f_nvl2(txt_escape,"","\\"+txt_escape))
    opt_hdfs_options = add_db_option(opt_hdfs_options, "quote", txt_enclose)
    opt_hdfs_options = add_db_option(opt_hdfs_options, "compression", hdfs_compression)
    opt_hdfs_options = opt_hdfs_options + f_nvl2(v_part_col_str,"", ".partitionBy(" + v_part_col_str + ")")
    opt_hdfs_options = opt_hdfs_options + ".save('" + hdfs_tgt_dir + "', mode='" + hdfs_load_type + "')"
    print(opt_hdfs_options)
    write_df=eval(opt_hdfs_options)
    
    target_hive=ctl_params.get("target_hive","")
    if target_hive!="":
        hive_create_table=target_hive.get("hive_create_table","N")
        hive_table_type=target_hive.get("hive_table_type","external") 
        hive_table=target_hive.get("hive_table","")
        hive_schema=target_hive.get("hive_schema","abi_repo")
        if hive_create_table=="Y":
            write_schema=src_df.schema
            col_nm=[]
            col_type=[]
            part_col_nm=[]
            part_col_type=[]
            for struct in write_schema:
                v_col_nm=struct.name
                v_col_type=struct.dataType
                
                col_type_map={"ArrayType":"Array<string>",
                "BinaryType":"Binary",
                "BooleanType":"Boolean",
                "CalendarIntervalType":"String",
                "DateType":"Date",
                "HiveStringType":"String",
                "MapType":"Map <string,string>",
                "NullType":"String",
                "NumericType":"DECIMAL",
                "ObjectType":"String",
                "StringType":"String",
                "StructType":"String",
                "TimestampType":"Timestamp",
                "ByteType":"String",
                "DoubleType":"Double",
                "IntegerType":"Integer",
                "LongType":"bigint",
                "ShortType":"Integer",
                "UnknownType":"String"}                
                v_col_type=col_type_map.get(str(v_col_type),"String")                
                if v_col_nm in tgt_partition_column_list:
                    part_col_nm.append(v_col_nm)
                    part_col_type.append(v_col_type)
                else:
                    col_nm.append(v_col_nm)
                    col_type.append(v_col_type) 
        
            v_col_str=""
            v_col_only_str=""
            for counter,col in enumerate(col_nm):
                v_col_str=f_nvl2(v_col_str, "", v_col_str + ", " ) + str(col) + " " + str(col_type[counter])
                v_col_only_str=f_nvl2(v_col_only_str, "", v_col_only_str + ", " ) + str(col)        
            v_part_col_str=""
            for counter,col in enumerate(part_col_nm):
                v_part_col_str=f_nvl2(v_part_col_str, "", v_part_col_str + ", " ) + str(col) + " " + str(part_col_type[counter])                              
        
        
            hive_db_result=hc.sql("show databases").select("databaseName").collect()
            hive_db_list=[]
            for hive_db in  hive_db_result:
                hive_db_list.append(hive_db.databaseName)
            
            if hive_schema not in hive_db_list:
                ignore_df=hc.sql("create database " + hive_schema)
        
            if hive_table_type=="managed":
                v_ddl="create table " + hive_schema + "." + hive_table + f_nvl2(tgt_partition_columns,"", "partitioned by (" + v_part_col_str + ") ") + v_txt_params + " STORED AS " + hdfs_format 
                print(v_ddl)
                hc.sql(v_ddl)
                src_df.registerTempTable("src_df");
                
                insert_sql="insert into " + hive_schema + "." + hive_table + f_nvl2(tgt_partition_columns,""," partition (" + tgt_partition_columns + ") ") + " select " + v_col_only_str + tgt_partition_columns + " from  src_df "  
                print(insert_sql) 
                hc.sql(insert_sql)
                
            else:
                v_hive_table_type="external"
                if hdfs_format=="text":
                    v_txt_params="ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ( 'separatorChar' = '" + txt_field_term + "','quoteChar' = '\\" + '"' + "', 'escapeChar' = '\\" + txt_escape + "')"
                    v_ddl="create " + v_hive_table_type + " table IF NOT EXISTS " +  hive_schema + "." + hive_table + " (" + v_col_str + ") " + f_nvl2(tgt_partition_columns,"","partitioned by (" + v_part_col_str + ") ")+v_txt_params+" STORED AS textfile location '" +hdfs_tgt_dir+"' "                 
                elif hdfs_format=="json":
                    v_txt_params=" ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' "
                    v_ddl="create " + v_hive_table_type + " table IF NOT EXISTS " + hive_schema + "." + hive_table + " (" + v_col_str + ") " + f_nvl2(tgt_partition_columns,"","partitioned by (" + v_part_col_str + ") ")+v_txt_params+" STORED AS textfile location '" +hdfs_tgt_dir+"' "  
                else:
                    v_txt_params=""   
                    v_ddl="create " + v_hive_table_type + " table IF NOT EXISTS " + hive_schema + "." + hive_table + " (" + v_col_str + ") " + f_nvl2(tgt_partition_columns,"","partitioned by (" + v_part_col_str + ") ")+v_txt_params+" STORED AS " + hdfs_format +" location '" +hdfs_tgt_dir+"' "
                print(v_ddl)    
                hc.sql(v_ddl)
                
            if tgt_partition_columns!="":
                v_msck="msck repair table " + hive_schema + "." + hive_table ;
                hc.sql(v_msck);
                
#--Main-----------------------------------

ctl_params=init_args()
source_cnx_nm=ctl_params["source_cnx_nm"]
source_cnx_info=get_cnx_list(source_cnx_nm)
src_df=get_src_df(source_cnx_info, ctl_params)
write_df=get_write_df(src_df,ctl_params)
