# Spark4n6

To run Spark4n6 on physical or virtual hardware, instead of on the Amazon
Web Services (AWS) cloud, please skip to **Building** below.

## Quick Start

With an account set up for running Amazon Elastic Compute Cloud (EC2) instances
on Virtual Private Cloud (VPC), starting a cluster will be a lot easier.  If
not, go to **Configuration** below first.

The **start-cluster.cmd** script will start an Elastic MapReduce (EMR) cluster
with one (1) m1.xlarge master node and fifteen (15) m3.xlarge core nodes using
spot pricing.  With spot pricing, there is a risk that the cluster could get
terminated, but it's much less expensive.  If any of the steps below do not
make sense, please do not start a cluster.

1. Upload images for testing to a Simple Storage Service (S3) bucket.
2. Download and install the Amazon AWS command-line interface.  The directory
    path to the **aws** command may need to be added to the search path,
    $PATH (UNIX) or %Path% (Windows).
3. Download and extract the spark4n6 zip file or use **git** to check out this
    repository.
4. Run **start-cluster.cmd** in the bin directory.

    * It runs **aws configure** to allow entry of AWS access credentials and defaults.

    ```
    C:\spark4n6>.\bin\start-cluster.cmd
    AWS Access Key ID [********************]:
    AWS Secret Access Key [********************]:
    Default region name [us-east-1]:
    Default output format [json]:
    ```
   * Then, the S3 buckets in the account are listed.  Please enter the name of
     the S3 bucket that contains forensic images for testing, and to which logs
     should be written. *s3://mybucket/* was chosen in the example below.

    ```
    BUCKETS 2014-03-16T22:46:36.000Z        mybucket
    BUCKETS 2016-03-02T16:55:22.000Z        spark4n6-public
    S3 bucket name:mybucket
    ```
   * Then, please enter the path in the bucket to which AWS should write 
     server logs, e.g. *s3://mybucket/logs*.
    ```
    Path to log files in the bucket:logs
    ```
   * Then, the SSH key pairs for EC2 in the account are listed, e.g. *my-key*
    ```
    KEYPAIRS        ke:y_:fi:ng:er:pr:in:t_:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx
    my-key
    EC2 SSH key pair name:my-key
    ```
   * Then the VPC subnets in the account will be listed.  Enter the desired
     subnet ID, e.g. *subnet-fce248a4* in the example below. This sets the 
     availability zone to *us-east-1d* in this example, which affects the 
     spot price.
    ```
    SUBNETS us-east-1b      251     10.0.0.0/24     False   False   available
    subnet-47b1a36f vpc-bfe153da
    TAGS    Name    Public subnet b
    SUBNETS us-east-1c      251     10.0.1.0/24     False   False   available
    subnet-2f940759 vpc-bfe153da
    TAGS    Name    Public subnet c
    SUBNETS us-east-1d      251     10.0.2.0/24     False   False   available
    subnet-fce248a4 vpc-bfe153da
    TAGS    Name    Public subnet d
    SUBNETS us-east-1e      251     10.0.3.0/24     False   False   available
    subnet-1aa4d027 vpc-bfe153da
    TAGS    Name    Public subnet e
    VPC subnet Id:subnet-fce248a4
    ```
   * Enter the spot bid price for the cluster's instances.
    ```
    EC2 Spot Instance Bid Price:0.25
    ```
   * If all of the above are successful, a new cluster will be started, and
     the cluster ID returned.  The cluster ID can be used with the 
     *aws emr terminate-clusters* command below to terminate the cluster.
    ```
    {
        "ClusterId": "j-3RMKNQFKJXU4T"
    }
    ***
    *** Cluster started.
    *** Costs will accrue until this cluster is terminated.
    *** To terminate the cluster, you can substitute the cluster ID above
    *** into the command below, or use the AWS console.
    *****
    ***** aws emr terminate-clusters --cluster-ids j-2WFXXXXXXX6ID
    *****
    ```
5. As the cluster is building, the Elastic MapReduce web console will list the
    public DNS name of the master node.  SSH to the master node.
6. Use **tmux** to get a window.  *tmux* allows multiple windows and splitting
    the SSH screen.  It also prevents jobs from crashing if
    there should be a temporary loss of connectivity.
    ```
    [hadoop@ip-10-0-2-207 ~]$ tmux
    ```
7. Execute **test.sh**.
   * The S3 buckets in the account are listed.  Please
   enter the name of the S3 bucket that contains the forensic test image to use
   and its path in the bucket, as shown below.
    ```
    [hadoop@ip-10-0-2-207 ~]$ spark4n6/bin/test.sh
    BUCKETS 2014-03-16T22:46:36.000Z        mybucket
    BUCKETS 2016-03-02T16:55:22.000Z        spark4n6-public
    S3 bucket name:mybucket
    2014-09-23 03:46:14 1572727021 images/CDrive.E01
    2014-09-23 03:42:03 1572717644 images/DDrive.E01
    Path to test image: images/CDrive.E01
    ```
  * Logs with command timing will
     be left in the current directory as runNx-Mgb, where N is the number of
     executors to be run on the cluster, and M is the number of GB of memory for
     each.
     ```
     [hadoop@ip-10-0-2-207 ~]$ ls -ltr
     total 518820
     drwxr-xr-x 6 hadoop hadoop      4096 Dec  3 01:27 activator-dist-1.3.7
     -rw-r--r-- 1 hadoop hadoop 526720809 Dec  3 01:28 typesafe-activator-1.3.7.zip
     drwxr-xr-x 7 hadoop hadoop      4096 Mar  7 04:21 spark4n6
     -rw-r--r-- 1 hadoop hadoop    635285 Mar  7 05:21 run8x-4gb.log
     -rw-r--r-- 1 hadoop hadoop    634782 Mar  7 05:40 run8x-8gb.log
     -rw-r--r-- 1 hadoop hadoop    650948 Mar  7 05:59 run16x-2gb.log
     -rw-r--r-- 1 hadoop hadoop    650519 Mar  7 06:19 run16x-4gb.log
     -rw-r--r-- 1 hadoop hadoop   1281278 Mar  7 06:32 run8x-2gb.log
     -rw-r--r-- 1 hadoop hadoop    649030 Mar  7 06:35 run16x-8gb.log
     [hadoop@ip-10-0-2-207 ~]$ tail run16x-8gb.log
     SLF4J: Found binding in [jar:file:/home/hadoop/.versions/2.4.0-amzn-7/share/hadoop/common/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
     SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
     16/03/07 06:35:39 INFO Configuration.deprecation: hadoop.native.lib is deprecated. Instead, use io.native.lib.available
     16/03/07 06:35:44 INFO spark4n6.HBaseSHA1:  0.10 GiB read, 100.00% complete
     CDrive.E01 = cb86d7777eda9baa2e2d99f09810c46ea635e3ce

     real    0m8.270s
     user    0m9.052s
     sys     0m1.868s
     HBaseSHA1.hash
     ```
8. The console also has instructions for accessing the web consoles for
services like Spark, Ganglia performance monitoring and the HBase database
through an SSH tunnel.

## Configuration

Before starting a cluster on Amazon Elastic MapReduce, you will need

### An S3 Bucket


#### Input Images
#### Logging Directory
### A Virtual Private Cloud
Set up a subnet in each availability zone in your region
### An SSH key pair for EC2
### Some Other Elastic MapReduce Settings
#### Visible To All Users
