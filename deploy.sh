#!/bin/bash

/bin/cp hadoop-common-project/hadoop-common/target/hadoop-common-2.4.0.jar /data/hadoop-2.4.0-bin/share/hadoop/common/hadoop-common-2.4.0.jar
/bin/cp hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-2.4.0.jar /data/hadoop-2.4.0-bin/share/hadoop/hdfs/hadoop-hdfs-2.4.0.jar
for i in {2..6}
do
    scp /data/hadoop-2.4.0-bin/share/hadoop/common/hadoop-common-2.4.0.jar hadoop-$i:/data/hadoop-2.4.0-bin/share/hadoop/common/hadoop-common-2.4.0.jar
    scp /data/hadoop-2.4.0-bin/share/hadoop/hdfs/hadoop-hdfs-2.4.0.jar hadoop-$i:/data/hadoop-2.4.0-bin/share/hadoop/hdfs/hadoop-hdfs-2.4.0.jar
done
