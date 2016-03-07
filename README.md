# Spark4n6

To run Spark4n6 on physical or virtual hardware, instead of on the Amazon
Web Services (AWS) cloud, please skip to **Building** below.

## Quick Start

With an account set up for running Amazon Elastic Compute Cloud (EC2) instances
on Virtual Private Cloud (VPC), starting a cluster will be a lot easier.  If
not, go to **Configuration** below first.

The **start-cluster.cmd** script will start a cluster with one (1) m1.xlarge
master node and fifteen (15) m3.xlarge core nodes using spot pricing.  With
spot pricing, there is a risk that the cluster could get terminated, but it's
much less expensive.  If any of the steps below do not make sense, please do
not start a cluster.

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
