
江门移动项目Hive代码
=====

# hive示例
## 1.将查询保存至路径
insert overwrite directory '/data/jm/result/user_id' <br>
row format delimited fields terminated by ','  <br>
select distinct(t.user_id) from jm_source t ,jm_jmda_station j  <br>
where t.station = j.station <br>

## 2.创建新表并从路径中读取文件 <br>
create external table jm_jmda_id(user_id string) <br>
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' <br>
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\") <br>
location '/data/jm/result/user_id/'

## 3 <br>
insert overwrite  directory '/data/jm/result/jmda_all' <br>
row format delimited fields terminated by ',' <br>
select j.* from jm_source j ,jm_jmda_id s  <br>
where j.user_id = s.user_id <br>

## 4 <br>
CREATE EXTERNAL TABLE `jm_jmda_all`( <br>
  `year` string COMMENT 'from deserializer',  <br>
  `month` string COMMENT 'from deserializer',  <br>
  `day` string COMMENT 'from deserializer',   <br>
  `hour` string COMMENT 'from deserializer',  <br>
  `user_id` string COMMENT 'from deserializer',  <br>
  `start_time` string COMMENT 'from deserializer',  <br>
  `end_time` string COMMENT 'from deserializer',  <br>
  `station` string COMMENT 'from deserializer',  <br>
  `ditrict` string COMMENT 'from deserializer') <br>
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' <br>
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\") <br>
location '/data/jm/result/jmda_all/' <br>

## 5 <br>
insert overwrite directory '/data/jm/result/all_station2' row format delimited fields terminated by ','  <br>
select distinct user_id from jm_jmda_all where station like concat("江门碧桂园盛景（微小I）","%")  <br>
                                       or station like concat("江门篁庄公园(从江门利家城拉远)","%")  <br>
                                       or station like concat("江门碧桂园员工宿舍","%")  <br>
                                       or station like concat("江门群星","%")   <br>
                                       or station like concat("江门篁庄大道西","%")  <br>
                                       or station like concat("江门龙马里","%")  <br>
                                       or station like concat("江门群华","%") <br>
                                       or station like concat("江门群华大厦","%") <br>

## 6 <br>									   
create external table jm_jmda_id2( user_id string) <br>
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' <br>
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\") <br>
location '/data/jm/result/all_station2/' <br>

## 7 <br>
insert overwrite  directory '/data/jm/result/jmda_all2' row format delimited fields terminated by ',' <br>
select j.* from jm_jmda_all j ,jm_jmda_id2 s where j.user_id = s.user_id <br>
 
## 8 <br>
create external table jm_jmda_all2(  <br>
  `year` string COMMENT 'from deserializer',  <br>
  `month` string COMMENT 'from deserializer',  <br>
  `day` string COMMENT 'from deserializer', `hour` string COMMENT 'from deserializer',  <br>
  `user_id` string COMMENT 'from deserializer', `start_time` string COMMENT 'from deserializer',  <br>
  `end_time` string COMMENT 'from deserializer', `station` string COMMENT 'from deserializer',  <br>
  `ditrict` string COMMENT 'from deserializer')  <br>
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' <br>
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\") <br>
location '/data/jm/result/jmda_all2/' <br>

