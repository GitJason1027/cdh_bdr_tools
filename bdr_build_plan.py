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

#tblist = ["sdm.absa_scrtz_ast_rpay_add","sdm.abss_ast_clbk_add","sdm.SASAF_CHT_CASE_TBL_ALL"]
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


TARGRT_CM_HOST = "180.137.10.63"

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

hdfs_args = ApiHdfsReplicationArguments(None)
#hdfs_args.sourceService = ApiServiceRef(None,peerName=PEER_NAME,clusterName=SOURCE_CLUSTER_NAME,serviceName=SOURCE_HIVE_SERVICE)
hdfs_args.mapreduceServiceName = 'yarn'
hdfs_args.userName = run_user

hive_args = ApiHiveReplicationArguments(None)
hive_args.sourceService = ApiServiceRef(None,peerName=PEER_NAME,clusterName=SOURCE_CLUSTER_NAME,serviceName=SOURCE_HIVE_SERVICE)
hive_args.hdfsArguments = hdfs_args
hive_args.tableFilters = plan_tblist

start = datetime.datetime.now()
end = start + datetime.timedelta(days=365)

if plan_name == '':
	plan_name = "API-"+start.strftime('%Y%m%d%H%M')

#只跑一次
schedule = hive.create_replication_schedule(start,None,None,0,True,hive_args,name=plan_name,description="API-PLAN-"+start.strftime('%Y%m%d%H%M'))


print("[INFO]完成")
