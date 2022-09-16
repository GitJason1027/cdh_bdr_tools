#!/usr/bin/env python
# -*-coding:UTF-8 -*-
import os
from cm_api.api_client import ApiResource
from cm_api.endpoints.types import *
import datetime
import sys

plan_name = sys.argv[1]
tblist_path = sys.argv[2]

cm_user = "xx"
cm_pwd = "xx#"
run_user = "xx"

print("[INFO]生成计划 "+plan_name+" -- 基于"+tblist_path)

with open(tblist_path,'r') as fr:
	tblist = fr.readlines()
plan_tblist = []

for tbn in tblist:
	_tbn = tbn.replace('\n','').strip().lower()
	if _tbn == '':
		continue
	hivetb = ApiHiveTable(None)
	hivetb.database=_tbn.split('.')[0]
	hivetb.tableName=_tbn.split('.')[1]
	plan_tblist.append(hivetb)


TARGRT_CM_HOST = "xx" #目标CM所在的ip

#目标的cm
api_root = ApiResource(TARGRT_CM_HOST,username=cm_user,password=cm_pwd)
cm = api_root.get_cloudera_manager()

PEER_NAME='PROD'

#源
SOURCE_CLUSTER_NAME='cluster'
SOURCE_HIVE_SERVICE='hive'

#目标
TARGET_CLUSTER_NAME='cluster'
TARGET_HIVE_SERVICE='hive'

TARGET_YARN_SERVICE = 'yarn'

hive = api_root.get_cluster(TARGET_CLUSTER_NAME).get_service(TARGET_HIVE_SERVICE)

hdfs args = ApiHdfsReplicationArguments(None) 
hdfs_args.mapreduceServiceName = 'yarn' 
hdfs_args.userName = run_user
hdfs_args.preservePermissions = True      # 保留权限 
hdfs_args.preserveBlockSize = True        # 保留块大小
hdfs_args.preserveReplicationCount = False# 副本计数
hdfs_args.replicationStrategy = 'DYNAMIC' # 复制策略:动态方式
hdfs_args.removeMissingFiles = True       # *删除策略:True删除到回收站，FFLSE 永文化
hdfs_args.skipTrash = True                # 跳过回收站，与上行True搭配使用代表永久助除 

hive_args = ApiHiveReplicationArguments(None)
hive_args.sourceService = ApiServiceRef(None,peerName=PEER_NAME,clusterName=SOURCE_CLUSTER_NAME,serviceName=SOURCE_HIVE_SERVICE)
hive_args.hdfsArguments = hdfs_args
if !plan_tblist:
	print("[ERROR]列表不能为空！！")
	exit(1)
hive_args.tableFilters = plan_tblist	  # 这个参数不能为None或者[]，为空的话会把整个hive进行同步，可能导致重大故障！！！！前面务必加上判断过滤空值情况！！！
hive_args.force = True                    # 强制覆盖
hive_args.runInvelidateMetadata = True    # 刷新impala元
hive_args.replicateData = True            # 复制HDFS文件

start = datetime.datetime.now()
end = start + datetime.timedelta(days=365)

if plan_name == '':
	plan_name = "API-"+start.strftime('%Y%m%d%H%M')

#创建计划
schedule = hive.create_replication_schedule(start,None,None,0,True,hive_args,name=plan_name,description="API-PLAN-"+start.strftime('%Y%m%d%H%M'))
#执行计划
apicommand = hive.trigger_replication_schdule(schedule.id)

print("[INFO]完成")
