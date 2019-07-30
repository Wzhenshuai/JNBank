

setHiveStr = "\r\r set hive.merge.mapfiles=true;\r" \
             "set hive.merge.mapredfiles=true;\r" \
             "set hive.enforce.bucketing = true;\r" \
             "set hive.exec.dynamic.partition=true;\r" \
             "set hive.exec.dynamic.partition.mode=nonstrict;\r" \
             "SET hive.exec.max.dynamic.partitions=100000;\r" \
             "SET hive.exec.max.dynamic.partitions.pernode=100000;\r"