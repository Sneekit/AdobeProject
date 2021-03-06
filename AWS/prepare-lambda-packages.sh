# !/bin/bash

# this script prepares the paramiko package for use as a Lambda Layer in AWS and uploads to S3
# connect to EC2 prior to running this script, then add layer from S3 bucket in Lambda

# directory stucture needed by Lambda
BUILDDIR="python/lib/python3.7/site-packages"
mkdir -p $BUILDDIR
pip3 install paramiko -t $BUILDDIR/
pip3 install cryptography -t $BUILDDIR/
pip3 install netmiko -t $BUILDDIR/
zip -r ParamikoPackage.zip .
aws s3api put-object --bucket adobe-project --key packages/ParamikoPackage.zip --body ParamikoPackage.zip
