CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`machine_learning_curated` (
  `user` string,
  `timeStamp` bigint,
  `x` float,
  `y` float,
  `z` float,
  `sensorReadingTime` bigint,
  `serialNumber` string,
  `distanceFromObject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://nagy-stedi-lakehouse/machine_learning/curated/'
TBLPROPERTIES ('classification' = 'json');