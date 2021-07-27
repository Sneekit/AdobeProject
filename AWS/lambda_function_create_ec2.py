# this Lambda function code takes the S3 file that was uploaded, and sends a command to EC2 to process it

import json
import urllib.parse
import boto3
import paramiko

def lambda_handler(event, context):
	print("Received event: " + json.dumps(event, indent = 2))

	# Get the object from the event
	bucket = event['Records'][0]['s3']['bucket']['name']
	key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding = "utf-8")
	sshkeyfile = "keys/adobe-ec2.pem"
	s3file = f"{bucket}/{key}"
	s3client = boto3.client("s3")

	# quick confirmation that it is a valid accessible file
	try:
		s3client.get_object(Bucket=bucket, Key=key)
	except Exception as ex:
		print(f"Error getting object {key} from bucket {bucket}. Make sure they exist and your bucket is in the same region as this function.")
		print(ex)
		raise ex

	# download key file needed for ssh connection
	print("Getting Key from S3")
	try:
		s3client.download_file(bucket, sshkeyfile, "/tmp/keyfile.pem")
	except Exception as ex:
		print(f"Error getting key from S3. Make sure {sshkeyfile} exists and your bucket is in the same region as this function.")
		print(ex)
		raise ex
	
	# create new EC2 instance
	print("Creating new EC2")
	try:
		ec2 = boto3.resource("ec2")
		
		instance = ec2.create_instances(
		    ImageId = "ami-0233c2d874b811deb",
		    MinCount = 1,
		    MaxCount = 1,
		    # can set an instance type based on the size of the file
		    InstanceType = "t2.micro",
		    Key = "adobe-ec2",
		    InstanceInitiatedShutdownBehavior = "terminate"
		)
		instance[0].wait_until_running()
		
	except Exception as ex:
		print(f"Error creating EC2 instance.")
		print(ex)
		raise ex	

	# connect to EC2
	print("Connection to EC2")
	key = paramiko.RSAKey.from_private_key_file("/tmp/keyfile.pem")
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
	host = instance[0].public_ip_address
	try:
		ssh.connect(hostname = host, username = "ec2-user", pkey = key)
	except Exception as ex:
		print(f"Error Connecting to EC2: {host}.")
		print(ex)
		raise ex

	# send EC2 the command to process the file
	command = f"sudo shutdown -h -P +240; cd AdobeProject; python3 ProcessFile.py {s3file} &"
	print(f"Sending Command: {command}")

	try:
		stdin, stdout, stderr = ssh.exec_command(command)
	except Exception as ex:
		print(f"Error executing command: {command}.")
		print(ex)
		raise ex

	out = stdout.read()
	err = stdout.read()
	if len(err):
		print("Command was submitted but EC2 returned an error")
		print(err)
		raise
	else:
		print(f"Successfully submitted to EC2: {command}.")

	# the Lambda has completed it's task successfully
	return {"Status Code": 200}
