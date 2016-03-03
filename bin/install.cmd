
aws configure
aws s3 ls s3://4n6/images --recursive | find ".E01"
set /p keyPairName=EC2 SSH key pair name:
aws emr create-cluster ^
   --name "Spark4n6 Cluster" ^
   --ami-version 3.11.0 ^
   --instance-groups ^
       Name="Spark4n6Master",InstanceCount=1,InstanceGroupType=MASTER,InstanceType=m1.large ^
       Name="Spark4n6 Core",InstanceCount=8,InstanceGroupType=CORE,InstanceType=m2.2xlarge ^
   --service-role EMR_DefaultRole ^
   --ec2-attributes KeyName=%keyPairName%,InstanceProfile=EMR_EC2_DefaultRole ^
   --bootstrap-actions ^
       Name="Install Ganglia",Path=s3://elasticmapreduce/bootstrap-actions/install-ganglia ^
       Name="Install Spark",Path=file:///usr/share/aws/emr/install-spark/install-spark ^
       Name="Install HBase",Path=s3://elasticmapreduce/bootstrap-actions/setup-hbase ^
       Name="Configure Hadoop",Args=["--hdfs-key-value","io.file.buffer.size=65536"],Path=s3://elasticmapreduce/bootstrap-actions/configure-hadoop ^
       Name="Configure daemons",Args=["--namenode-opts=-XX:GCTimeRatio=19"],Path=s3://elasticmapreduce/bootstrap-actions/configure-daemons
       Name="Initialize the cluster",Args=["--namenode-opts=-XX:GCTimeRatio=19"],Path=s3://elasticmapreduce/bootstrap-actions/configure-daemons
