ds=$(date -d '1 hour ago' '+%Y%m%d %H')
partisi_ds=$(echo $ds |cut -d' ' -f 1)
partisi_periode=$(echo $ds | cut -d' ' -f 2)

rfolder=/home/sqm_app/airflow/dags/script/alarm_omnibus/trusted_production
beeline -u "jdbc:hive2://jt-hdp02i0102.telkom.co.id:2181,jt-hdp02i0103.telkom.co.id:2181,jt-hdp02i0407.telkom.co.id:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;ssl=true" -e "INSERT OVERWRITE TABLE trusted.oss_alarm_omnibus PARTITION(ds='$partisi_ds',periode='${partisi_periode}00',sourcetype='insert') SELECT identifier, node, nodealias, manager, agent, alertgroup, alertkey, severity, summary, firstoccurrence, lastoccurrence, internallast, poll, type, tally, probesubsecondid, class, grade, location, owneruid, ownergid, acknowledged, flash, eventid, expiretime, processreq, suppressescl, customer, service,physicalslot, physicalport, physicalcard, tasklist, nmosserial, nmosobjinst, nmoscausetype, nmosdomainname, nmosentityid, nmosmanagedstatus, nmoseventmap, localnodealias, localpriobj, localsecobj, localrootobj, remotenodealias, remotepriobj, remotesecobj, remoterootobj, x733eventtype, x733probablecause, x733specificprob, x733corrnotif, url, extendedattr, collectionfirst, aggregationfirst, mediator, originalseverity, clearedby, clearedat, nmosprecedence, nmosentityname, nmosentitytype, devicefunction, devicelocation, devicevendor, deviceregion, deviceclass, interfacename, interfacedescr, devicesubregion, devicezone, devicenetwork, ttticketstatus, ttlogticket, tterrorcode, ttnumber, ttstate, ttgammas, ttworkzone, itmstatus, itmdisplayitem, itmeventdata, itmtime, itmhostname, itmport, itminttype, itmresetflag, itmsittype, itmthrunode, itmsitgroup, itmsitfullname, itmappllabel, itmsitorigin, techostname, tecfqhostname, tecdate, tecrepeatcount, siaserviceaffecting, siaresourceid, siastatus, siaserviceids, siaservicecount, siaservicearea, siaservicename, siacategory, siasubcategory, provisosubelementindex, provisothresholdtype, provisoviolationtime, provisosubelementgrpindex, provisoformulaindex, provisoformulavalue, provisometricname, servername, serverserial FROM staging.oss_omnibus_insert WHERE from_unixtime(unix_timestamp(lastoccurrence, 'MM/dd/yy HH:mm:ss'),'yyyyMMddHH00')='$partisi_ds${partisi_periode}00';"
#bash $rfolder/omnibus_akses.sh > $rfolder/omnibus_akses.log 2>&1 &
#bash $rfolder/omnibus_broadband.sh > $rfolder/omnibus_broadband.log 2>&1 &
#bash $rfolder/omnibus_core.sh > $rfolder/omnibus_core.log 2>&1 &
#bash $rfolder/omnibus_cpe.sh > $rfolder/omnibus_cpe.log 2>&1 &
#bash $rfolder/omnibus_dwdm.sh > $rfolder/omnibus_dwdm.log 2>&1 &
#bash $rfolder/omnibus_metro.sh > $rfolder/omnibus_metro.log 2>&1 &
#bash $rfolder/omnibus_voice.sh > $rfolder/omnibus_voice.log 2>&1 &