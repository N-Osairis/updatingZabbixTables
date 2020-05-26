# updatingZabbixTables

This script provided to push the tables data from zabbix(HQ_clone) to platformdb(pro) server:

**Note:
 The script automatically, access to the platform server and stop the platform script 
 and then will update the tables and once updating finish it  will restart the platform script. 

List of zabbix tbales:
1)'acknowledges'
2)'actions'
3)'alerts'
4)'drules'
5)'events'
6)'functions'
7)'graphs'
8)'graphs_items'
9)'host_inventory'
10)'hosts'
11)'interface'
12)'items'
13)'maintenances'
14)'network_zabbixgraphs'
15)'triggers'
16)'users'
17)'valuemaps'

