ds=$(date -d '1 hour ago' '+%Y%m%d %H')
partisi_ds=$(echo $ds |cut -d' ' -f 1)
partisi_periode=$(echo $ds | cut -d' ' -f 2)


#omnibus_metro_insert_to_production
#beeline -u "jdbc:hive2://jt-hdp02i0402.telkom.co.id:2181,jt-hdp02i0403.telkom.co.id:2181,jt-hdp02i0301.telkom.co.id:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;principal=hive/_HOST@HDPTELKOM.CO.ID;ssl=true;sslTrustStore=/home/hive/truststore.jks;trustStorePassword=4dm1ndb" -e "INSERT OVERWRITE TABLE production.oss_alarm_omnibus_metro PARTITION(ds='$partisi_ds',periode='${partisi_periode}00',tipe_perangkat='METRO',sourcetype='insert') SELECT identifier, node, nodealias, manager, agent, alertgroup, alertkey, severity, summary, firstoccurrence, lastoccurrence, internallast, poll, type, tally, probesubsecondid, class, grade, location, customer, service, physicalslot, physicalport, physicalcard, collectionfirst, aggregationfirst, mediator, originalseverity, clearedat, devicefunction, devicelocation, devicevendor, deviceregion, devicesubregion, devicezone, devicenetwork, ttticketstatus, ttnumber, ttstate, ttgammas, ttworkzone, siaserviceaffecting, siaresourceid, siastatus, siaservicecount, siacategory, siasubcategory, servername, serverserial FROM tusted.oss_alarm_omnibus WHERE (agent='Alcatel 5620 SAM' OR TRIM(agent)='alcatel_5620_sam_v10' OR TRIM(agent)='U2KKBL:172.31.109.2' OR TRIM(agent)='U2KPHL:172.31.31.34' OR TRIM(agent)='U2KPTR:172.31.233.2') AND from_unixtime(unix_timestamp(lastoccurrence, 'MM/dd/yy HH:mm:ss'),'yyyyMMddHH00')='$partisi_ds${partisi_periode}00' AND sourcetype='insert';" 
beeline -u "jdbc:hive2://jt-hdp02i0402.telkom.co.id:2181,jt-hdp02i0403.telkom.co.id:2181,jt-hdp02i0301.telkom.co.id:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;principal=hive/_HOST@HDPTELKOM.CO.ID;ssl=true;sslTrustStore=/home/hive/truststore.jks;trustStorePassword=4dm1ndb" -e "INSERT OVERWRITE TABLE production.oss_alarm_omnibus_metro PARTITION(ds='$partisi_ds',periode='${partisi_periode}00',sourcetype='insert') SELECT identifier, node, nodealias, manager, agent, alertgroup, alertkey, severity, summary, firstoccurrence, lastoccurrence, internallast, poll, type, tally, probesubsecondid, class, grade, location, customer, service, physicalslot, physicalport, physicalcard, collectionfirst, aggregationfirst, mediator, originalseverity, clearedat, devicefunction, devicelocation, devicevendor, deviceregion, devicesubregion, devicezone, devicenetwork, ttticketstatus, ttnumber, ttstate, ttgammas, ttworkzone, siaserviceaffecting, siaresourceid, siastatus, siaservicecount, siacategory, siasubcategory, servername, serverserial FROM trusted.oss_alarm_omnibus WHERE (agent='Alcatel 5620 SAM' OR TRIM(agent)='alcatel_5620_sam_v10' OR TRIM(agent)='U2KKBL:172.31.109.2' OR TRIM(agent)='U2KPHL:172.31.31.34' OR TRIM(agent)='U2KPTR:172.31.233.2') AND ds='$partisi_ds' AND periode='${partisi_periode}00' AND sourcetype='insert';"

#insert_to_es
#beeline -u "jdbc:hive2://jt-hdp02i0402.telkom.co.id:2181,jt-hdp02i0403.telkom.co.id:2181,jt-hdp02i0301.telkom.co.id:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;principal=hive/_HOST@HDPTELKOM.CO.ID;ssl=true;sslTrustStore=/home/hive/truststore.jks;trustStorePassword=4dm1ndb" -e "add jar hdfs:////data/trusted/oss/elasticsearch-hadoop-7.3.0.jar;SET hive.execution.engine=MR;INSERT OVERWRITE TABLE trusted.oss_es_alarm_omnibus_metro_new SELECT node, nodealias, agent, alertkey, severity, summary, from_unixtime(unix_timestamp(firstoccurrence, 'MM/dd/yy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as datetime, from_unixtime(unix_timestamp(lastoccurrence, 'MM/dd/yy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as lastoccurrence, physicalslot, physicalport, physicalcard, tally, siasubcategory, ds FROM production.oss_alarm_omnibus_metro where from_unixtime(unix_timestamp(lastoccurrence, 'MM/dd/yy HH:mm:ss'),'yyyyMMddHH00')='$partisi_ds${partisi_periode}00';"

