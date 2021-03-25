import paramiko
import datetime as dt
import time
import pymysql
from DBUtils.SteadyDB import connect
# =======================================
db_tables = ['acknowledges', 'actions', 'alerts', 'drules', 'events',
             'functions', 'graphs', 'graphs_items', 'host_inventory',
             'hosts', 'interface', 'items', 'maintenances',
             'network_zabbixgraphs', 'triggers', 'users', 'valuemaps'
             ]


# Database credentials(zabbix _AWS)
source_cred = {
    "host": "18.136.66.177",
    "port": 13306,
    "user": "reza",
    "passwd": "ZAQ!2wsx8262",
    "database": 'zabbix'
 }

# =======================================
# Database credentials(CLONE ZABBIX- HQ)
# source_cred = {
#     "host": "202.168.69.200",
#     "port": 13345,
#     "user": "al.thahirie",
#     "passwd": "ZAQ!2wsx8262",
#     "database": 'zabbix'
# }
# =======================================
# # SSH server credentials(platform_db)
platformscrip_ssh_cred = {
    "host": "18.136.66.177",
    "port": 10424,
    "user": "centos",
    "passwd": "PioiJWMqo0ouvDeN"
}

# # Database credentials(platform_db)
dest_cred = {
    "host": "18.136.66.177",
    "port": 10408,
    "user": "reza",
    "passwd": "ZAQ!2wsx8262",
    "database": 'platformDB'
}
# =======================================
# # SSH server credentials(platform_db_test)
# platformscrip_ssh_cred = {
#     "host": "18.136.66.177",
#     "port": 10423,
#     "user": "centos",
#     "passwd": "zaq12wsx8262"
# }
# # Database credentials(platform_db_test)
# dest_cred = {
#     "host": "18.136.66.177",
#     "port": 10407,
#     "user": "reza",
#     "passwd": "ZAQ!2wsx8262",
#     "database": 'platformDB'
# }
# ========================================
# input arg='stop' or 'start'


def change_platform_script(st):
    comment = None
    status_comment = 'sudo systemctl status platform'
    if 'stop' in st:
        comment = 'sudo systemctl stop platform'
    elif 'start' in st:
        comment = 'sudo systemctl start platform'
    if comment is not None:
        print('{}-----sudo systemctl' + st + 'platform'.format(dt.datetime.now()))
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(platformscrip_ssh_cred['host'], platformscrip_ssh_cred['port'],
                    platformscrip_ssh_cred['user'], platformscrip_ssh_cred['passwd'])
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(comment, bufsize=-1,
                                                             timeout=None, get_pty=False, environment=None)
        outlines = ssh_stdout.readlines()
        resp = ''.join(outlines)
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(status_comment, bufsize=-1,
                                                             timeout=None, get_pty=False, environment=None)
        outlines = ssh_stdout.readlines()
        resp = ''.join(outlines)
        print(resp)
        ssh.close()
        print('---------------------------------------------')


def create_value_string(x):
    i = 1
    v = None
    while i <= x:
        if i == 1:
            v = " values( %s"
        else:
            v = v+",%s"
        i = i+1

    v = v+")"
    return v


print("------------------------------------- ".format(dt.datetime.now(), len(db_tables)))
print("{}------Scripte Started".format(dt.datetime.now()))

change_platform_script('stop')
src_db = connect(creator=pymysql, host=source_cred['host'], port=source_cred['port'],
                 user=source_cred['user'], password=source_cred['passwd'], database=source_cred['database'],
                 autocommit=True, charset='utf8mb4',
                 cursorclass=pymysql.cursors.Cursor)
src_cursor = src_db.cursor()
des_db = connect(creator=pymysql, host=dest_cred['host'], port=dest_cred['port'],
                 user=dest_cred['user'], password=dest_cred['passwd'], database=dest_cred['database'],
                 autocommit=True, charset='utf8mb4',
                 cursorclass=pymysql.cursors.Cursor)
des_cursor = des_db.cursor()


for ind, table in enumerate(db_tables):
    print("({}/{})-{}------started to Truncate table: {} ".format(ind + 1, len(db_tables), dt.datetime.now(), table))
    des_cursor.execute('SET FOREIGN_KEY_CHECKS = 0;')
    des_cursor.execute('TRUNCATE platformDB.' + table)
print("{}------All the tables ({} tables) Truncated successfully "
      "at platformDB ".format(dt.datetime.now(), len(db_tables)))
print("------------------------------------------".format(dt.datetime.now(), len(db_tables)))

for ind, table in enumerate(db_tables):
    print("({}/{})-{}------started to copy table: {} ".format(ind + 1, len(db_tables), dt.datetime.now(), table))
    print("      Processing, please wait…")
    query1 = "SELECT count(*)  FROM information_schema.columns WHERE table_schema ='{}' " \
             "and table_name ='{}';".format(source_cred['database'], table)
    src_cursor.execute(query1)
    col_no = src_cursor.fetchall()
    query2 = "select * from  zabbix." + table
    src_cursor.execute(query2)
    result = src_cursor.fetchall()
    src_db.commit()
    time.sleep(3)
    nc = col_no[0][0]
    valuesString = str(create_value_string(nc))
    query3 = 'INSERT INTO platformDB.' + table + valuesString
    des_cursor.executemany(query3, result)
    des_db.commit()
change_platform_script('start')
print("{}------All the tables ({} tables) copied successfully from  "
      "zabbix(HQ) to platformDB ".format(dt.datetime.now(), len(db_tables)))

