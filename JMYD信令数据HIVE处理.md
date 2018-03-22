#hive语句
##1将查询保存至路径
insert overwrite directory '/data/jm/result/user_id' 
row format delimited fields terminated by ',' 
select distinct(t.user_id) from jm_source t ,jm_jmda_station j 
where t.station = j.station

#2创建新表并从路径中读取文件
create external table jm_jmda_id(user_id string)
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/user_id/'

#3
insert overwrite  directory '/data/jm/result/jmda_all' 
row format delimited fields terminated by ','
select j.* from jm_source j ,jm_jmda_id s 
where j.user_id = s.user_id

#4
CREATE EXTERNAL TABLE `jm_jmda_all`(
  `year` string COMMENT 'from deserializer', 
  `month` string COMMENT 'from deserializer', 
  `day` string COMMENT 'from deserializer', 
  `hour` string COMMENT 'from deserializer', 
  `user_id` string COMMENT 'from deserializer', 
  `start_time` string COMMENT 'from deserializer', 
  `end_time` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/jmda_all/'

#5
insert overwrite directory '/data/jm/result/all_station2' row format delimited fields terminated by ',' 
select distinct user_id from jm_jmda_all where station like concat("江门碧桂园盛景（微小I）","%") 
                                       or station like concat("江门篁庄公园(从江门利家城拉远)","%") 
                                       or station like concat("江门碧桂园员工宿舍","%") 
                                       or station like concat("江门群星","%") 
                                       or station like concat("江门篁庄大道西","%") 
                                       or station like concat("江门龙马里","%") 
                                       or station like concat("江门群华","%") 
                                       or station like concat("江门群华大厦","%")

#6									   
create external table jm_jmda_id2( user_id string)
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/all_station2/'

#7
insert overwrite  directory '/data/jm/result/jmda_all2' row format delimited fields terminated by ','
select j.* from jm_jmda_all j ,jm_jmda_id2 s where j.user_id = s.user_id

#8
create external table jm_jmda_all2( 
  `year` string COMMENT 'from deserializer', 
  `month` string COMMENT 'from deserializer', 
  `day` string COMMENT 'from deserializer', `hour` string COMMENT 'from deserializer', 
  `user_id` string COMMENT 'from deserializer', `start_time` string COMMENT 'from deserializer', 
  `end_time` string COMMENT 'from deserializer', `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer') 
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\") 
location '/data/jm/result/jmda_all2/'

##------------------8亿多条记录-----------------------
#1
insert overwrite  directory '/data/jm/result/statistics' 
row format delimited fields terminated by ','
select count(*) as count 
from jm_jmda_all2

#2
CREATE EXTERNAL TABLE M(count int)
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/statistics/'

select * from M

############筛选8-11点
insert overwrite directory '/data/jm/result/jm_jmda_all2_811' 
row format delimited fields terminated by ',' 
select * from jm_jmda_all2 where hour>=8 and hour<=11

create external table jm_jmda_all2_811( 
  `year` string COMMENT 'from deserializer', 
  `month` string COMMENT 'from deserializer', 
  `day` string COMMENT 'from deserializer', `hour` string COMMENT 'from deserializer', 
  `user_id` string COMMENT 'from deserializer', `start_time` string COMMENT 'from deserializer', 
  `end_time` string COMMENT 'from deserializer', `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer') 
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\") 
location '/data/jm/result/jm_jmda_all2_811/'

#########经过江门大道+经过两路口+8-11时 的有98033人
insert overwrite directory '/data/jm/result/statistic/id_num1' 
row format delimited fields terminated by ',' 
select count(distinct user_id) as count from jm_jmda_all2_811

CREATE EXTERNAL TABLE id_num1(count int)
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/statistic/id_num1'

select * from id_num1

#########经过江门大道+在8-11时经过两路口 的有45673人
insert overwrite directory '/data/jm/result/statistic/id_num2' 
row format delimited fields terminated by ',' 
select count(distinct user_id) as count from jm_jmda_all2_811 where station like concat("江门碧桂园盛景（微小I）","%") 
                                       or station like concat("江门篁庄公园(从江门利家城拉远)","%") 
                                       or station like concat("江门碧桂园员工宿舍","%") 
                                       or station like concat("江门群星","%") 
                                       or station like concat("江门篁庄大道西","%") 
                                       or station like concat("江门龙马里","%") 
                                       or station like concat("江门群华","%") 
                                       or station like concat("江门群华大厦","%")
									   
CREATE EXTERNAL TABLE id_num2(count int)
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/statistic/id_num2'

select * from id_num2
#########经过江门大道+经过两路口+8-11时 的有200514915条记录
insert overwrite directory '/data/jm/result/statistic/record_num1' 
row format delimited fields terminated by ',' 
select count(*) as count from jm_jmda_all2_811 

CREATE EXTERNAL TABLE record_num1(count int)
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/statistic/record_num1'

select * from record_num1
#########经过江门大道+在8-11时经过两路口 的有6956602条记录
insert overwrite directory '/data/jm/result/statistic/record_num2' 
row format delimited fields terminated by ',' 
select count(*) as count from jm_jmda_all2_811 where station like concat("江门碧桂园盛景（微小I）","%") 
                                       or station like concat("江门篁庄公园(从江门利家城拉远)","%") 
                                       or station like concat("江门碧桂园员工宿舍","%") 
                                       or station like concat("江门群星","%") 
                                       or station like concat("江门篁庄大道西","%") 
                                       or station like concat("江门龙马里","%") 
                                       or station like concat("江门群华","%") 
                                       or station like concat("江门群华大厦","%")

CREATE EXTERNAL TABLE record_num1(count int)
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/statistic/record_num2'

select * from record_num2

############################
########给jm_jmda_all2的基站匹配镇区标签：tag_area

CREATE EXTERNAL TABLE `jiangmendadao_station_area`(
  `station` string COMMENT 'from deserializer',  
  `tag_area` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/jiangmendadao_station_area/'

select * from jiangmendadao_station_area

insert overwrite  directory '/data/jm/result/jm_daoall2_areatags/' 
row format delimited fields terminated by ','  
select a.*,b.tag_area from jm_jmda_all2 a left outer join jiangmendadao_station_area b on a.station=b.station

CREATE EXTERNAL TABLE `jm_daoall2_areatags`(
  `year` string COMMENT 'from deserializer', 
  `month` string COMMENT 'from deserializer', 
  `day` string COMMENT 'from deserializer', 
  `hour` string COMMENT 'from deserializer', 
  `user_id` string COMMENT 'from deserializer', 
  `start_time` string COMMENT 'from deserializer', 
  `end_time` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer',
   `tag_area` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/jm_daoall2_areatags/';

########给小时匹配时段标签：hour_inter

CREATE EXTERNAL TABLE `jiangmendadao_hour_inter`(
  `hour` string COMMENT 'from deserializer',  
  `hour_inter` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/time_inter/';


insert overwrite  directory '/data/jm/result/jm_daoall2_areatags2/' 
row format delimited fields terminated by ','  
select a.*,b.hour_inter from jm_daoall2_areatags a left outer join jiangmendadao_hour_inter b on a.hour=b.hour;

CREATE EXTERNAL TABLE `jm_daoall2_areatags2`(
  `year` string COMMENT 'from deserializer', 
  `month` string COMMENT 'from deserializer', 
  `day` string COMMENT 'from deserializer', 
  `hour` string COMMENT 'from deserializer', 
  `user_id` string COMMENT 'from deserializer', 
  `start_time` string COMMENT 'from deserializer', 
  `end_time` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer',
   `tag_area` string COMMENT 'from deserializer',
  `hour_inter` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/jm_daoall2_areatags2/';

########给基站匹配江门大道标签：index_


insert overwrite  directory '/data/jm/result/jm_daoall2_areatags3/' 
row format delimited fields terminated by ','  
select a.*,b.index_ from jm_daoall2_areatags2 a left outer join jm_jmda_station b on a.station=b.station;

CREATE EXTERNAL TABLE `jm_daoall2_areatags3`(
  `year` string COMMENT 'from deserializer', 
  `month` string COMMENT 'from deserializer', 
  `day` string COMMENT 'from deserializer', 
  `hour` string COMMENT 'from deserializer', 
  `user_id` string COMMENT 'from deserializer', 
  `start_time` string COMMENT 'from deserializer', 
  `end_time` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer',
   `tag_area` string COMMENT 'from deserializer',
  `hour_inter` string COMMENT 'from deserializer',
 `index_` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/jm_daoall2_areatags3/';

########给基站匹配所属大道路口标签：road

CREATE EXTERNAL TABLE `stations_2road`(
  `station` string COMMENT 'from deserializer',  
  `road` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/stations_2road/';

insert overwrite  directory '/data/jm/result/jm_daoall2_areatags4/' 
row format delimited fields terminated by ','  
select a.*,b.road from jm_daoall2_areatags3 a left outer join stations_2road b on a.station=b.station;

CREATE EXTERNAL TABLE `jm_daoall2_areatags4`(
  `year` string COMMENT 'from deserializer', 
  `month` string COMMENT 'from deserializer', 
  `day` string COMMENT 'from deserializer', 
  `hour` string COMMENT 'from deserializer', 
  `user_id` string COMMENT 'from deserializer', 
  `start_time` string COMMENT 'from deserializer', 
  `end_time` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer',
   `tag_area` string COMMENT 'from deserializer',
  `hour_inter` string COMMENT 'from deserializer',
`index_` string COMMENT 'from deserializer',
`road` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/jm_daoall2_areatags4/';

#########提取start_time的分钟,字段为minute

insert overwrite  directory '/data/jm/result/jm_daoall2_tags4/' 
row format delimited fields terminated by ','  
select year,
month,
day,
hour,
minute(start_time) as minute,
user_id,
start_time,
end_time,
station,
ditrict,
tag_area,
hour_inter,
index_,
road  from jm_daoall2_areatags4;

CREATE EXTERNAL TABLE `jm_daoall2_tags4`(
  `year` string COMMENT 'from deserializer', 
  `month` string COMMENT 'from deserializer', 
  `day` string COMMENT 'from deserializer', 
  `hour` string COMMENT 'from deserializer', 
  `minute` string COMMENT 'from deserializer',
  `user_id` string COMMENT 'from deserializer', 
  `start_time` string COMMENT 'from deserializer', 
  `end_time` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer',
   `tag_area` string COMMENT 'from deserializer',
  `hour_inter` string COMMENT 'from deserializer',
`index_` string COMMENT 'from deserializer',
`road` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/jm_daoall2_tags4/';

#########数据集按ID，按时间先后排序

insert overwrite  directory '/data/jm/result/jm_daoall2_tags4_sort/' 
row format delimited fields terminated by ','  
select user_id,
year,
month,
day,
hour,
minute,
start_time,
end_time,
station,
ditrict,
tag_area,
hour_inter,
index_,
road from jm_daoall2_tags4 order by user_id,int(month),int(day),int(hour),int(minute),station;

CREATE EXTERNAL TABLE `jm_daoall2_tags4_sort`(
`user_id` string COMMENT 'from deserializer', 
  `year` string COMMENT 'from deserializer', 
  `month` string COMMENT 'from deserializer', 
  `day` string COMMENT 'from deserializer', 
  `hour` string COMMENT 'from deserializer', 
  `minute` string COMMENT 'from deserializer',
  `start_time` string COMMENT 'from deserializer', 
  `end_time` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer',
   `tag_area` string COMMENT 'from deserializer',
  `hour_inter` string COMMENT 'from deserializer',
`index_` string COMMENT 'from deserializer',
`road` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/jm_daoall2_tags4_sort/';

###去重清洗
insert overwrite  directory '/data/jm/result/daoall2_tags4_sort_t/' 
row format delimited fields terminated by ','
select  
user_id,
 min(unix_timestamp(start_time)) as start_tamp,
 max(unix_timestamp(end_time)) as end_tamp,
station,
ditrict,
tag_area,
hour_inter,
index_,
road from jm_daoall2_tags4_sort group by user_id,station,ditrict,tag_area,hour_inter,index_,road ;


CREATE EXTERNAL TABLE `daoall2_tags4_sort_t`(
  `user_id` string COMMENT 'from deserializer', 
   `start_tamp` string COMMENT 'from deserializer',
  `end_tamp` string COMMENT 'from deserializer',
  `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer',
   `tag_area` string COMMENT 'from deserializer',
  `hour_inter` string COMMENT 'from deserializer',
`index_` string COMMENT 'from deserializer',
`road` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/daoall2_tags4_sort_t/';

######一步加3个标签并转时间字段为时间戳，排序

insert overwrite  directory '/data/jm/result/jm_source_tags3/' 
row format delimited fields terminated by ','  
select e.*,f.road from 
       (select c.*,d.index_ from 
                (select a.user_id,unix_timestamp(a.start_time) as start_stamp,
                 unix_timestamp(a.end_time) as end_stamp,a.station,a.ditrict,b.tag_area from 
                jm_source a left outer join jiangmendadao_station_area b on a.station=b.station
                 ) 
        c left outer join jm_jmda_station d on c.station=d.station
         ) 
e left outer join stations_2road f on e.station=f.station order by user_id,start_stamp,end_stamp,station;


CREATE EXTERNAL TABLE `jm_source_tags3`(
  `user_id` string COMMENT 'from deserializer', 
  `start_stamp` string COMMENT 'from deserializer', 
  `end_stamp` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer',
   `tag_area` string COMMENT 'from deserializer',
`index_` string COMMENT 'from deserializer',
`road` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/jm_source_tags3/';

########代码改进
insert overwrite  directory '/data/jm/result/jm_source_tags3/' 
row format delimited fields terminated by ','  

select a.user_id,unix_timestamp(a.start_time) as start_stamp,unix_timestamp(a.end_time) as end_stamp,
       a.station,h.lng,h.lat,a.ditrict,b.tag_area,d.index_,f.road from jm_source a 
	                        left outer join jiangmendadao_station_area b on a.station=b.station
                            left outer join jm_jmda_station d on a.station=d.station
                            left outer join stations_2road f on a.station=f.station 
							left outer join station_lnglat h on a.station=h.station 
							order by user_id,start_stamp,end_stamp,station;

CREATE EXTERNAL TABLE `jm_source_tags3`(
  `user_id` string COMMENT 'from deserializer', 
  `start_stamp` string COMMENT 'from deserializer', 
  `end_stamp` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
   `lng` string COMMENT 'from deserializer',
  `lat` string COMMENT 'from deserializer',
  `ditrict` string COMMENT 'from deserializer',
   `tag_area` string COMMENT 'from deserializer',
`index_` string COMMENT 'from deserializer',
`road` string COMMENT 'from deserializer'
)
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/jm_source_tags3/';

#####4idTEST
insert overwrite  directory '/data/jm/result/jmsource_4id/' 
row format delimited fields terminated by ','  
select * from jm_source where user_id in ['811078025544915','811002443262645','811002608346736', '811078167875023'];

CREATE EXTERNAL TABLE `jmsource_4id`(
  `year` string COMMENT 'from deserializer', 
  `month` string COMMENT 'from deserializer', 
  `day` string COMMENT 'from deserializer', 
  `hour` string COMMENT 'from deserializer', 
  `user_id` string COMMENT 'from deserializer', 
  `start_time` string COMMENT 'from deserializer', 
  `end_time` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/jmsource_4id/';

insert overwrite  directory '/data/jm/result/jmsource_id4_tags4/' 
row format delimited fields terminated by ','  
select a.user_id,a.hour,unix_timestamp(a.start_time) as start_stamp,unix_timestamp(a.end_time) as end_stamp,
       a.station,h.lng,h.lat,a.ditrict,b.tag_area,d.index_,f.road from jmsource_4id a 
	                    left outer join jiangmendadao_station_area b on a.station=b.station
                            left outer join jm_jmda_station d on a.station=d.station
                            left outer join stations_2road f on a.station=f.station 
			    left outer join station_lnglat h on a.station=h.station 
			    order by user_id,start_stamp,end_stamp,station;



CREATE EXTERNAL TABLE `jmsource_id4_tags4`(
  `user_id` string COMMENT 'from deserializer', 
   `hour` string COMMENT 'from deserializer', 
  `start_stamp` string COMMENT 'from deserializer', 
  `end_stamp` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
   `lng` string COMMENT 'from deserializer',
  `lat` string COMMENT 'from deserializer',
  `ditrict` string COMMENT 'from deserializer',
   `tag_area` string COMMENT 'from deserializer',
`index_` string COMMENT 'from deserializer',
`road` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/jmsource_id4_tags4/';




######tag
CREATE EXTERNAL TABLE `test`(
  `user_id` string COMMENT 'from deserializer', 
  `start_stamp` string COMMENT 'from deserializer', 
  `end_stamp` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer',
   `tag_area` string COMMENT 'from deserializer',
`index_` string COMMENT 'from deserializer',
`road` string COMMENT 'from deserializer',
  `index2` string COMMENT 'from deserializer',
   `state` string COMMENT 'from deserializer'
)
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/practice/jm_daoall2_tags3/000000_0_format/result/';




#####从jm_source中只抽某一天413
CREATE EXTERNAL TABLE `jmsource_413`(
  `year` string COMMENT 'from deserializer', 
  `month` string COMMENT 'from deserializer', 
  `day` string COMMENT 'from deserializer', 
  `hour` string COMMENT 'from deserializer',
  `user_id` string COMMENT 'from deserializer', 
  `start_time` string COMMENT 'from deserializer', 
  `end_time` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
  `ditrict` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('escapeChar'='\\','quoteChar'='\"','separatorChar'='\u0001') 
location '/data/jm/test/4-13/';




insert overwrite  directory '/data/jm/result/jmsource_413_tag/' 
row format delimited fields terminated by ','  
select a.user_id,a.hour,unix_timestamp(a.start_time) as start_stamp,unix_timestamp(a.end_time) as end_stamp,
       a.station,b.lng,b.lat,a.ditrict,b.area,b.block,b.stationtype,b.coverarea,b.coversence,d.index_,f.road from jmsource_413 a 
	                    left outer join station6 b on a.station=b.station
                            left outer join jm_jmda_station d on a.station=d.station
                            left outer join stations_2road f on a.station=f.station 
			    order by user_id,start_stamp,end_stamp,station;
				


				
CREATE EXTERNAL TABLE `jmsource_413_tag`(
  `user_id` string COMMENT 'from deserializer', 
   `hour` string COMMENT 'from deserializer', 
  `start_stamp` string COMMENT 'from deserializer', 
  `end_stamp` string COMMENT 'from deserializer', 
  `station` string COMMENT 'from deserializer', 
   `lng` string COMMENT 'from deserializer',
  `lat` string COMMENT 'from deserializer',
  `ditrict` string COMMENT 'from deserializer',
   `area` string COMMENT 'from deserializer',
   `block` string COMMENT 'from deserializer',
   `stationtype` string COMMENT 'from deserializer',
   `coverarea` string COMMENT 'from deserializer',
   `coversence` string COMMENT 'from deserializer',
`index_` string COMMENT 'from deserializer',
`road` string COMMENT 'from deserializer')
row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/jmsource_413_tag/';

########
create external table jm_413_analyse_result(
     user_id string,
      from_ string,
     to_ string)

row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/practice/jm_413_analyse_result/'
##15241条记录
select count(*) as ss from jm_413_analyse_result

##1600用户
select count(distinct user_id) as ss from jm_413_analyse_result

##93from
select count(distinct from_) from jm_413_analyse_result

##96to
select count(distinct to_) from jm_413_analyse_result

###
create external table jm_413_result(
     user_id string,
      from_ string,
     to_ string,
     road_type int,
     road_consume string,
     stay_consume string,
     start_time string,
     arrive_time string
   )

row format SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with SERDEPROPERTIES ("separatorChar" = ",","quoteChar"= '"',"escapeChar"= "\\")
location '/data/jm/result/spark/analyse/'