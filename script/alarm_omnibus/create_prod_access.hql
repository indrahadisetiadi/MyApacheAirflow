 CREATE EXTERNAL TABLE production.oss_alarm_omnibus_access(           
   `identifier` string,                                      
   `node` string,                                            
   `nodealias` string,                                       
   `manager` string,                                         
   `agent` string,                                           
   `alertgroup` string,                                      
   `alertkey` string,                                        
   `severity` string,                                        
   `summary` string,                                         
   `firstoccurrence` string,                                 
   `lastoccurrence` string,                                  
   `internallast` string,                                    
   `poll` string,                                            
   `type` string,                                            
   `tally` string,                                           
   `probesubsecondid` string,                                
   `class` string,                                           
   `grade` string,                                           
   `location` string,                                        
   `customer` string,                                        
   `service` string,                                         
   `physicalslot` string,                                    
   `physicalport` string,                                    
   `physicalcard` string,                                    
   `collectionfirst` string,                                 
   `aggregationfirst` string,                                
   `mediator` string,                                        
   `originalseverity` string,                                
   `clearedat` string,                                       
   `devicefunction` string,                                  
   `devicelocation` string,                                  
   `devicevendor` string,                                    
   `deviceregion` string,                                    
   `devicesubregion` string,                                 
   `devicezone` string,                                      
   `devicenetwork` string,                                   
   `ttticketstatus` string,                                  
   `ttnumber` string,                                        
   `ttstate` string,                                         
   `ttgammas` string,                                        
   `ttworkzone` string,                                      
   `siaserviceaffecting` string,                             
   `siaresourceid` string,                                   
   `siastatus` string,                                       
   `siaservicecount` string,                                 
   `siacategory` string,                                     
   `siasubcategory` string,                                  
   `servername` string,                                      
   `serverserial` string)                                    
 PARTITIONED BY (                                            
   `ds` string,                                              
   `periode` string,                                         
   `sourcetype` string)                                      
 ROW FORMAT SERDE                                            
   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'               
 STORED AS INPUTFORMAT                                       
   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'         
 OUTPUTFORMAT                                                
   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'        
 LOCATION                                                    
   '/data/production/oss/alarm_omnibus_access'  
 TBLPROPERTIES (                                             
   'orc.compress'='SNAPPY',                                  
   'transient_lastDdlTime'='1573194651')                     
