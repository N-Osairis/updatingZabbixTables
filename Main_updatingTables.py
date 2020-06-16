import paramiko
import datetime as dt
import time
import pymysql
from DBUtils.SteadyDB import connect
#=======================================
DB_Tables=['acknowledges','actions','alerts','drules','events',
           'functions','graphs','graphs_items','host_inventory',
           'hosts','interface','items','maintenances',
           'network_zabbixgraphs','triggers','users','valuemaps'
           ]
#SSH server credentials(platform_db)
platformscripSSH_CRED={
    "host": "18.136.66.177",
    "port": 10424,
    "user": "centos",
    "passwd": "zaq12wsx8262"
}
# Database credentials(CLONE ZABBIX- HQ)
SOURCE_CRED = {
    "host": "175.139.176.181",
    "port": 13345,
    "user": "al.thahirie",
    "passwd": "ZAQ!2wsx8262",
    "database" :'zabbix'
}
# Database credentials(platform_db)
# DEST_CRED = {
#     "host": "18.136.66.177",
#     "port": 10408,
#     "user": "reza",
#     "passwd": "ZAQ!2wsx8262",
#     "database": 'platformDB'
# }
# Database credentials(platform_db_test)
DEST_CRED = {
    "host": "18.136.66.177",
    "port": 10407,
    "user": "reza",
    "passwd": "ZAQ!2wsx8262",
    "database": 'platformDB'
}
#===============================================
#input arg='stop' or 'start'
def change_platformScript(st):
    comment=None
    status_comment='sudo systemctl status platform'
    if 'stop' in st:
        comment= 'sudo systemctl stop platform'
    elif 'start' in st:
        comment = 'sudo systemctl start platform'
    if comment !=None:
        print('{}-----sudo systemctl'+ st +'platform'.format(dt.datetime.now()))
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(platformscripSSH_CRED['host'], platformscripSSH_CRED['port'], platformscripSSH_CRED['user'], platformscripSSH_CRED['passwd'])
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(comment, bufsize=-1, timeout=None,get_pty=False, environment=None)
        outlines = ssh_stdout.readlines()
        resp = ''.join(outlines)
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(status_comment, bufsize=-1, timeout=None, get_pty=False, environment=None)
        outlines = ssh_stdout.readlines()
        resp = ''.join(outlines)
        print(resp)
        ssh.close()
        print('---------------------------------------------')
def createValuestring(x):
 i=1
 v=None
 while i<=x:
     if i==1:
        v= " values( %s"
     else:
        v=v+",%s"
     i=i+1

 v=v+")"
 return v
#================================================

print("----------------------------------------------------------------------- ".format(dt.datetime.now(),len(DB_Tables)))
print("{}------Scripte Started".format(dt.datetime.now()))
change_platformScript('stop')

src_db = connect( creator = pymysql, host= SOURCE_CRED['host'],port= SOURCE_CRED['port'],
                  user=SOURCE_CRED['user'], password=SOURCE_CRED['passwd'], database = SOURCE_CRED['database'],
                  autocommit = True, charset = 'utf8mb4',
                  cursorclass = pymysql.cursors.Cursor)
src_cursor = src_db.cursor()


des_db =connect( creator = pymysql, host= DEST_CRED['host'],port= DEST_CRED['port'],
                 user=DEST_CRED['user'], password=DEST_CRED['passwd'], database = DEST_CRED['database'],
                 autocommit = True, charset = 'utf8mb4',
                 cursorclass = pymysql.cursors.Cursor )
des_cursor = des_db .cursor()




for ind,table in enumerate( DB_Tables):
    print("({}/{})-{}------started to Truncate table: {} ".format(ind+1,len(DB_Tables),dt.datetime.now(),table))
    des_cursor.execute('SET FOREIGN_KEY_CHECKS = 0;')
    des_cursor.execute('TRUNCATE platformDB.' + table)
print("{}------All the tables ({} tables) Truncated successfully at platformDB ".format(dt.datetime.now(),len(DB_Tables)))
print("----------------------------------------------------------------------- ".format(dt.datetime.now(),len(DB_Tables)))

for ind,table in enumerate( DB_Tables):
    print("({}/{})-{}------started to copy table: {} ".format(ind+1,len(DB_Tables),dt.datetime.now(),table))
    print("      Processing, please wait…")
    query1="SELECT count(*)  FROM information_schema.columns WHERE table_schema ='{}' and table_name ='{}';".format(SOURCE_CRED['database'],table)
    src_cursor.execute(query1)
    col_no = src_cursor.fetchall()
    query2 = "select * from  zabbix." + table
    src_cursor.execute(query2)
    result = src_cursor.fetchall()
    src_db.commit()
    time.sleep(3)
    nc = col_no[0][0]
    valuesString= str(createValuestring(nc))
    query3 = 'INSERT INTO platformDB.' + table + valuesString
    des_cursor.executemany(query3, result)
    des_db.commit()
change_platformScript('start')
print("{}------All the tables ({} tables) copied successfully from  zabbix(HQ) to platformDB ".format(dt.datetime.now(),len( DB_Tables)))

