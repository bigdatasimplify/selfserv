import jaydebeapi as jdbc
import argparse
import json
import sys

ctl_dir='../ctl'
db_cfg_file='../config/db_config.json'
with open(db_cfg_file,'r') as cfg:
    db_cfg_json=json.load(cfg)   
    db_cnx_file=db_cfg_json["db_cnx_file"]	
    db_sql_file=db_cfg_json["db_sql_file"]
    db_jar_file=db_cfg_json["db_jar_file"]


def init_args():
  # Parse Arguments
  parser = argparse.ArgumentParser(description='List DB objects.')
  parser.add_argument('--cnx', dest='db_cnx_name', help='Enter DB Connection Name')
  parser.add_argument('--fetch', dest='db_oper', help='Enter DB Operation')
  parser.add_argument('--query', dest='db_query', help='Enter DB Query')
  args = parser.parse_args()
  return args
  
def list_cnx():
  # Parse Connection File
  with open(db_cnx_file,'r') as cf:
    v_cnx_list=[]
    for cnxline in cf:
       v_cnx_json=json.loads(cnxline)
       v_cnx=list(v_cnx_json.keys())
       v_cnx_list.append(v_cnx[0])
  return v_cnx_list

def save_cnx(object):
  with open(db_cnx_file,'a') as cf:
     cf.write('\n'+json.dumps(object))	

def gen_db_where(db_where_txt):
    if db_where_txt:
        db_where=""
        for counter,schema in enumerate(db_where_txt.split(",")):
            if counter==0:
               db_where = " schema_nm in ('"+ schema + "'"
            else:
               db_where = db_where + ",'"+ schema + "'"  	
        db_where = db_where + ")" 
    else:
        db_where = " 1=1 "
    return  db_where
	 
def gen_db_obj_sql(db_cnx_name,db_where):
  db_cnx_info=gen_db_cnx_info(db_cnx_name)
  db_type=db_cnx_info["db_type"]
  
  # Decide on SQL, based on fetch or custom query
  with open(db_sql_file,'r') as sqlf:
      v_sql_json=json.load(sqlf)
      db_sql=v_sql_json.get("listObjects").get(db_type)            
				
  if db_where:
      db_sql=db_sql + " " + db_where
  
  return db_sql
  
def gen_db_cnx_info(db_cnx_name):
  # Parse Connection File
  with open(db_cnx_file,'r') as cf:
    for cnxline in cf:
        v_cnx_json=json.loads(cnxline)
        if v_cnx_json.get(db_cnx_name):
           db_cnx_info=v_cnx_json[db_cnx_name]
           db_type=db_cnx_info["db_type"]	
  
  # Parse Jar File
  with open(db_jar_file,'r') as jf:
    v_jar_json=json.load(jf)
    db_jar=v_jar_json[db_type]
    db_cnx_info["db_jar"]=db_jar
    
  return db_cnx_info

  
def db_connect(db_cnx_info):
  db_driver=db_cnx_info["db_driver"]
  db_jdbc_url=db_cnx_info["db_jdbc_url"]
  db_properties=db_cnx_info["db_properties"]
  db_jar=db_cnx_info["db_jar"]
  conn = jdbc.connect(db_driver, db_jdbc_url, db_properties, db_jar)  
  return conn
  
def db_execute(conn, db_sql):
  curs = conn.cursor()
  curs.execute(db_sql)
  # list of table columns
  column_names = list(map(lambda x: x.lower(), [d[0] for d in curs.description]))
  # list of data items
  rows = list(curs.fetchall())
  result = [dict(zip(column_names, row)) for row in rows]
  curs.close()
  return result

if __name__ == "__main__":
  try:
      args=init_args()
      db_cnx_name=args.db_cnx_name
      db_oper=args.db_oper	  
      db_query=args.db_query
      
      db_cnx_info=gen_db_cnx_info(db_cnx_name=db_cnx_name)
      conn=db_connect(db_cnx_info)
      result=list(db_execute(conn=conn, db_sql=db_sql))
      print(result)
  except:
      raise
      sys.exit(1)  
  
 
  
  
