from db_execution import *
import os, sys

# Get Connection List
cnx_list=list_cnx()
print(cnx_list)

# Test Connection
db_cnx_name="pg_edw"
db_cnx_info=gen_db_cnx_info(db_cnx_name=db_cnx_name)
conn=db_connect(db_cnx_info)
if conn:
  print("Connection Successful")
else:
  print("Connection Failed")  

# Save Connection
object={"z":"c"}
save_cnx(object)
	
# Get List of Tables
db_cnx_name="pg_edw"
db_where_txt="edw"
db_where=gen_db_where(db_where_txt)
db_obj_sql=gen_db_obj_sql(db_cnx_name,db_where)
db_cnx_info=gen_db_cnx_info(db_cnx_name=db_cnx_name)
conn=db_connect(db_cnx_info)
table_list=list(db_execute(conn=conn, db_sql=db_obj_sql))
print(table_list)

#Display Batch Summary
batch_nm=
table_list=
hdfs_tgt_dir=
hive_schema=
batch_config=
sqoop_job_id=<generate a unique id>

for table in table_list:
  job_summary={} 
  sqoop_job_nm= batch_nm + "_" + table
  sqoop_job_type=batch_type  
  hdfs_tgt_dir = hdfs_tgt_dir + "/" + table.replace(".","_")
  hive_table = hive_schema + "." + table.replace(".","_")
  sqoop_job_config=batch_config
  sqoop_job_config.update({"sqoop_job_id":sqoop_job_id})   
  sqoop_job_config.update({"sqoop_job_nm":sqoop_job_nm})
  sqoop_job_config.update({"sqoop_job_type":sqoop_job_type})
  sqoop_job_config.update({"hdfs_tgt_dir":hdfs_tgt_dir})
  sqoop_job_config.update({"hive_table":hive_table})   
  sqoop_job_config_file=batch_nm + "_" + table.replace(".","_") + ".cfg"
  print(sqoop_job_id + "|" + sqoop_job_nm + "|" + json_dumps(sqoop_job_config) + "|" + sqoop_job_config_file ) 

# Wait for User Updates/Overrides
# Save Sqoop Job Configs  
for batch_job in batch_overrides:
  with open(sqoop_job_config_file,"w") as ctl:
     ctl.write(json.dumps(batch_job))
	 
	 
  