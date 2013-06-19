
ssh caoz@15.154.147.161

rm hadoop/hadoop_install/hadoop-core-1.0.1-SNAPSHORT.jar
rm hadoop/hadoop_install/hadoop-tools-1.0.1-SNAPSHORT.jar
exit

ssh caoz@15.154.147.162 
rm hadoop/hadoop_install/hadoop-core-1.0.1-SNAPSHORT.jar
rm hadoop/hadoop_install/hadoop-tools-1.0.1-SNAPSHORT.jar
exit

ssh caoz@15.154.147.163 
rm hadoop/hadoop_install/hadoop-core-1.0.1-SNAPSHORT.jar
rm hadoop/hadoop_install/hadoop-tools-1.0.1-SNAPSHORT.jar
exit

scp hadoop-core-1.0.1-SNAPSHORT.jar caoz@15.154.147.161:hadoop/hadoop_install/
scp hadoop-tools-1.0.1-SNAPSHORT.jar caoz@15.154.147.161:hadoop/hadoop_install/

scp hadoop-core-1.0.1-SNAPSHORT.jar caoz@15.154.147.162:hadoop/hadoop_install/
scp hadoop-tools-1.0.1-SNAPSHORT.jar caoz@15.154.147.162:hadoop/hadoop_install/

scp hadoop-core-1.0.1-SNAPSHORT.jar caoz@15.154.147.163:hadoop/hadoop_install/
scp hadoop-tools-1.0.1-SNAPSHORT.jar caoz@15.154.147.163:hadoop/hadoop_install/



