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

def get_cnx_list(source_cnx_nm):
    cnx_file="/home/hadoop/config/db_cnx.cfg"
    with open(cnx_file,'r') as cnx_file:
        cnx_list=json.load(cnx_file)
    source_cnx_info=next(cnx_item.values()[0] for cnx_item in cnx_list if cnx_item.keys()[0] == source_cnx_nm)    
    return source_cnx_info

def get_db_connect_query(source_cnx_info, ctl_params):
    db_jar=source_cnx_info.get("db_jar")
    db_jdbc_url=source_cnx_info.get("db_jdbc_url")
    db_user=source_cnx_info.get("db_properties").get("user")
    db_pass=source_cnx_info.get("db_properties").get("password")

    db_columns=ctl_params["source_extract"].get("db_columns","*")
    db_where_clause=ctl_params["source_extract"].get("db_where_clause","1=1")
    db_schema_nm=ctl_params["source_extract"].get("db_schema_nm")
    db_table_nm=ctl_params["source_extract"].get("db_table_nm")
    db_query="select "+ db_columns + " from " + db_schema_nm + "." + db_table_nm + " where " + db_where_clause

    opt_db_jar=" -files '" + db_jar + "' -libjars '" + db_jar + "'"
    opt_db_connect=" --connect '" + db_jdbc_url + "?currentschema=" + db_schema_nm + "' --username " + db_user + " --password '" + db_pass + "'"
    opt_db_query=" --query '" + db_query + " and $CONDITIONS '"
    opt_db_connect_query= opt_db_jar + " " + opt_db_connect + " " + opt_db_query
    return opt_db_connect_query

def get_split_mappers(ctl_params):
    db_split_columns=ctl_params["parallelism"].get("db_split_columns","")
    if db_split_columns:
       opt_split_columns=" --split-by '" + db_split_columns + "'"
    else:
       opt_split_columns=" "

    num_mappers=ctl_params["parallelism"].get("num_mappers","")
    if num_mappers:
       opt_num_map=" --num-mappers " + num_mappers
    else:
       opt_num_map=" "

    return opt_split_columns + " " + opt_num_map


def get_null_options(ctl_params):
    null_str_replace=ctl_params["null_options"].get("null_str_replace","")
    if null_str_replace:
       opt_null_str_replace=" --null-string '" + null_str_replace + "' "
    else:
       opt_null_str_replace=""

    null_nonstr_replace=ctl_params["null_options"].get("null_nonstr_replace","")
    if null_nonstr_replace:
       opt_null_nonstr_replace=" --null-non-string '" + null_nonstr_replace + "' "
    else:
       opt_null_nonstr_replace=""
    return opt_null_str_replace + " " + opt_null_nonstr_replace

def get_javamap(ctl_params):
    db_datatype_javamap=ctl_params["target_hdfs"].get("db_datatype_javamap","")
    if db_datatype_javamap:
       opt_datatype_javamap=" --map-column-java '" + db_datatype_javamap + "' "
    else:
       opt_datatype_javamap=""
    return opt_datatype_javamap

def get_hdfs_format_options(target_hdfs):
    tgt_data_format_type=target_hdfs.get("tgt_data_format_type","text")
    if tgt_data_format_type=="text":
       txt_field_term=target_hdfs.get("txt_field_term",",")
       txt_line_term=target_hdfs.get("txt_line_term","\n")
       txt_escape=target_hdfs.get("txt_escape","")
       txt_enclose=target_hdfs.get("txt_enclose","'")

       if txt_field_term:
          opt_txt_field_term=" --fields-terminated-by '" + txt_field_term + "' "
       else:
          opt_txt_field_term=""

       if txt_line_term:
          opt_txt_line_term=" --lines-terminated-by '" + txt_line_term + "' "
       else:
          opt_txt_line_term=""

       if txt_escape:
          opt_txt_escape=" --escaped-by '" + txt_escape + "' "
       else:
          opt_txt_escape=""

       if txt_enclose:
          opt_txt_enclose=" --optionally-enclosed-by \"" + txt_enclose + "\" "
       else:
          opt_txt_enclose=""

       opt_hdfs_format=" --as-textfile  " + opt_txt_field_term + " " + opt_txt_line_term + " " + opt_txt_escape + " " + opt_txt_enclose + " "

    elif tgt_data_format_type=="avro":
       opt_hdfs_format=" --as-avrodatafile "
    elif tgt_data_format_type=="parquet":
       opt_hdfs_format=" --as-parquetfile "
    elif tgt_data_format_type=="sequence":
       opt_hdfs_format=" --as-sequencefile "
    else:
       raise exception("Unexpected data format type. Only avro, parquet, sequence and text are supported"); 

    return opt_hdfs_format


def get_hdfs_compress_options(target_hdfs):
    compress_type=target_hdfs.get("compress_type","")
    if compress_type=="" or compress_type==None:
      opt_hdfs_compression=""
    elif compress_type=="gzip":
      compress_codec="org.apache.hadoop.io.compress.GzipCodec"
      opt_hdfs_compression=" --compress --compression-codec " + compress_codec
    elif compress_type=="snappy":
      compress_codec="org.apache.hadoop.io.compress.SnappyCodec"
      opt_hdfs_compression=" --compress --compression-codec " + compress_codec
    elif compress_type=="bzip2":
      compress_codec="org.apache.hadoop.io.compress.BZip2Codec"
      opt_hdfs_compression=" --compress --compression-codec " + compress_codec
    elif compress_type=="deflate":
      compress_codec="org.apache.hadoop.io.compress.DeflateCodec"
      opt_hdfs_compression=" --compress --compression-codec " + compress_codec
    elif compress_type=="lzo":
      compress_codec="com.hadoop.compression.lzo.LzopCodec"
      opt_hdfs_compression=" --compress --compression-codec " + compress_codec
    else:
      raise exception("Unexpected compression type. Only gzip, snappy, bzip2, defalte and lzo are supported");

    return opt_hdfs_compression

def get_hdfs_options(ctl_params):  
    target_hdfs=ctl_params["target_hdfs"]

    hdfs_load_type=target_hdfs.get("hdfs_load_type","")
    if hdfs_load_type=="":
       opt_hdfs_reload=""
    elif hdfs_load_type=="overwrite":
       opt_hdfs_reload=" --delete-target-dir "
    elif hdfs_load_type=="append":
       opt_hdfs_reload=" --append "
    else:
       opt_hdfs_reload=""
 
    opt_hdfs_tgt_dir=" --target-dir " + target_hdfs["hdfs_tgt_dir"]

    opt_hdfs_format=get_hdfs_format_options(target_hdfs)

    opt_hdfs_compression=get_hdfs_compress_options(target_hdfs)

    opt_hdfs_options = opt_hdfs_tgt_dir + " " + opt_hdfs_reload + " " + opt_hdfs_format  + " " + opt_hdfs_compression

    return opt_hdfs_options

def get_hivemap(ctl_params):
  target_hive=ctl_params["target_hive"]
  db_datatype_hivemap=target_hive.get("db_datatype_hivemap","")
  if db_datatype_hivemap:
     opt_datatype_hivemap=" --map-column-hive '" + db_datatype_hivemap + "' "
  else:
     opt_datatype_hivemap=""

  return opt_datatype_hivemap

def get_hive_options(ctl_params):
    target_hive=ctl_params["target_hive"]
    hive_table=target_hive.get("hive_table","")
    if hive_table=="":
       opt_hive_table=""
    else:
       opt_hive_table=" --hive-table " + hive_table

    hive_load_type=target_hive.get("hive_load_type","")
    if hive_load_type=="":
       opt_hive_reload=""
    elif hive_load_type=="overwrite":
       opt_hive_reload=" --hive-overwrite "
    elif hive_load_type=="append":
       opt_hive_reload="";

    hdfs_tgt_dir=target_hive.get("hdfs_tgt_dir","")
    if hdfs_tgt_dir=="":
       sqoop_job_nm=ctl_params["job"]["sqoop_job_nm"]
       opt_hdfs_tgt_dir=" --target-dir  /tmp/" + sqoop_job_nm
    else:
       opt_hdfs_tgt_dir=" --target-dir " + hdfs_tgt_dir

    opt_hdfs_format=get_hdfs_format_options(target_hive)
    opt_hdfs_compression=get_hdfs_compress_options(target_hive)

    hive_create_table=target_hive.get("hive_create_table","")
    if hive_create_table=="Y" or hive_create_table=="y":
       opt_hive_create_table=" --create-hive-table " + opt_hdfs_format + " " + opt_hdfs_compression
    else:
       opt_hive_create_table=" "  + opt_hdfs_format + " " + opt_hdfs_compression

    hive_part_key=target_hive.get("hive_part_key","")
    hive_part_val=target_hive.get("hive_part_val","")
    if hive_part_key!="" and hive_part_val!="":
       opt_hive_partition_settings=" --hive-partition-key " + hive_part_key + " --hive-partition-value '" +  hive_part_val + "' "
    else:
       opt_hive_partition_settings=""

    if hive_create_table=="Y" or hive_create_table=="y" or hive_load_type=="overwrite":
       opt_hdfs_tgt_dir=opt_hdfs_tgt_dir + " --delete-target-dir "
    else:
       opt_hdfs_tgt_dir=opt_hdfs_tgt_dir

    opt_hive_options= opt_hdfs_tgt_dir + " --hive-import " + "  " + opt_hive_table + "  " + opt_hive_reload + "  " + opt_hive_create_table + "  " + opt_hive_partition_settings
	
    return opt_hive_options

def get_increment_options(ctl_params):
    incremental_load=ctl_params.get("incremental_load","")
    if incremental_load:
       increment_type=incremental_load.get("increment_type","")
       increment_col=incremental_load.get("increment_col","")
       increment_last_val=incremental_load.get("increment_last_val","")

       merge_key=incremental_load.get("merge_key", "")
       if merge_key:
          increment_merge=" --merge-key " + merge_key
       else:
          increment_merge=""
 
       opt_increment_settings=" --incremental " + increment_type + " --check-column " + increment_col + " --last-value '" + str(increment_last_val) + "' " + increment_merge + " "
    else:
       opt_increment_settings=""

    return opt_increment_settings

def get_cmd_db2hdfs_reg():
    sqoop_cmd=" sqoop import " + opt_db_connect_query \
    + "  " + opt_null_options   \
    + "  " + opt_split_mappers   \
    + "  " + opt_datatype_javamap   \
    + "  " + opt_hdfs_options  
    return sqoop_cmd

def get_cmd_db2hdfs_increment():
    sqoop_cmd=" sqoop import " + opt_db_connect_query \
    + "  " + opt_null_options   \
    + "  " + opt_split_mappers   \
    + "  " + opt_datatype_javamap   \
    + "  " + opt_hdfs_options  \
    + "  " + opt_increment_options
    return sqoop_cmd

def get_cmd_db2hive_reg():
    sqoop_cmd=" sqoop import " + opt_db_connect_query \
    + "  " + opt_null_options   \
    + "  " + opt_split_mappers   \
    + "  " + opt_datatype_hivemap   \
    + "  " + opt_hive_options
    return sqoop_cmd

def get_cmd_db2hive_increment():
    sqoop_cmd=" sqoop import " + opt_db_connect_query \
    + "  " + opt_null_options   \
    + "  " + opt_split_mappers   \
    + "  " + opt_datatype_hivemap   \
    + "  " + opt_hive_options  \
    + "  " + opt_increment_options
    return sqoop_cmd

#--Main-----------------------------------
if True:
#try:
   ctl_params=init_args()
   source_cnx_nm=ctl_params["source_cnx_nm"]
   source_cnx_info=get_cnx_list(source_cnx_nm)
   
   opt_db_connect_query=get_db_connect_query(source_cnx_info, ctl_params)
   opt_split_mappers=get_split_mappers(ctl_params)
   opt_null_options=get_null_options(ctl_params)

   target_type=ctl_params.get("target_type")
   if target_type=="hdfs":
      opt_datatype_javamap=get_javamap(ctl_params)
      opt_hdfs_options=get_hdfs_options(ctl_params)
          
   elif target_type=="hive":
      opt_datatype_hivemap=get_hivemap(ctl_params)
      opt_hive_options=get_hive_options(ctl_params)

   opt_increment_options=get_increment_options(ctl_params)
   
   sqoop_job_type=ctl_params.get("job").get("sqoop_job_type")
   if sqoop_job_type=="db2hdfs_reg":
   
       sqoop_cmd=get_cmd_db2hdfs_reg()
   
   elif sqoop_job_type=="db2hdfs_increment":
  
       sqoop_cmd=get_cmd_db2hdfs_increment()
       if ctl_params.get("target_hdfs").get("hdfs_load_type","")=="overwrite":
           raise exception("Cannot perform incremental load when target overwrite is provided. Please modify ctl file");

   elif sqoop_job_type=="db2hive_reg":
   
       sqoop_cmd=get_cmd_db2hive_reg()
 
   elif sqoop_job_type=="db2hive_increment":

       sqoop_cmd=get_cmd_db2hive_increment()
      
       if ctl_params.get("target_hive").get("hive_load_type","")=="overwrite":
           raise exception("Cannot perform incremental load when target overwrite is provided. Please modify ctl file");
 
       if ctl_params.get("target_hive").get("hive_create_table")=="Y" or ctl_params.get("target_hive").get("hive_create_table")=="y":
           raise exception("Cannot perform incremental load with create hive table option. Please modify ctl file");

   print(sqoop_cmd) 
   os.system(sqoop_cmd)
    
#except:
#   print("Failed to execute")
#   os.sys.exit(1)  

#---------------------------------------
