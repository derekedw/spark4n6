
aws configure
aws s3api list-buckets --output text | find "BUCKETS"
set /p bucket=S3 bucket name:
:: aws s3 ls s3://%bucket% --recursive | find ".E01" | find /v ".txt"
:: set /p testImagePath=Path to test image:
set /p logPath=Path to log files in the bucket:
aws ec2 describe-key-pairs --output text
set /p keyPairName=EC2 SSH key pair name:
aws ec2 describe-subnets --output text
set /p subnet=VPC subnet Id:
set /p spotBidPrice=EC2 Spot Instance Bid Price:
aws emr create-cluster ^
   --name "Spark4n6 Cluster" ^
   --ami-version 3.11.0 ^
   --instance-groups ^
       Name="Spark4n6Master",InstanceCount=1,InstanceGroupType=MASTER,BidPrice=%spotBidPrice%,InstanceType=m1.xlarge ^
       Name="Spark4n6 Core",InstanceCount=15,InstanceGroupType=CORE,BidPrice=%spotBidPrice%,InstanceType=m3.xlarge ^
   --service-role EMR_DefaultRole ^
   --ec2-attributes KeyName=%keyPairName%,InstanceProfile=EMR_EC2_DefaultRole,SubnetId=%subnet% ^
   --log-uri s3://%bucket%/%logPath% ^
   --applications ^
           Name=PIG ^
           Name=HUE ^
           Name=SPARK ^
           Name=HBASE ^
   --bootstrap-actions ^
       Name="Install Ganglia",Path=s3://elasticmapreduce/bootstrap-actions/install-ganglia ^
       Name="Install Spark",Path=file:///usr/share/aws/emr/install-spark/install-spark ^
       Name="Install HBase",Path=s3://elasticmapreduce/bootstrap-actions/setup-hbase ^
       Name="Configure Hadoop",Args=["--hdfs-key-value","io.file.buffer.size=65536"],Path=s3://elasticmapreduce/bootstrap-actions/configure-hadoop ^
       Name="Configure daemons",Args=["--namenode-opts=-XX:GCTimeRatio=19"],Path=s3://elasticmapreduce/bootstrap-actions/configure-daemons ^
       Name="Patch the cluster's software and OS",Path=s3://spark4n6-public/bootstrap-actions/patchall.sh ^
       Name="Initialize the cluster",Path=s3://elasticmapreduce/bootstrap-actions/run-if,Args=["instance.isMaster=true",s3://spark4n6-public/bootstrap-actions/start.sh]

