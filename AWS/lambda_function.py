import json
import urllib.parse
import boto3
import paramiko

def lambda_handler(event, context):
	print("Received event: " + json.dumps(event, indent=2))

	# Get the object from the event
	bucket = event['Records'][0]['s3']['bucket']['name']
	key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
	sshkeyfile = "keys/adobe-ec2.pem"
	s3file = f"{bucket}/{key}"

	s3_client = boto3.client('s3')

	# quick confirmation that it is a valid accessible file
	try:
		s3_client.get_object(Bucket=bucket, Key=key)
	except Exception as ex:
		print(f"Error getting object {key} from bucket {bucket}. Make sure they exist and your bucket is in the same region as this function.")
		print(ex)
		raise ex

	print("Getting Key from S3")
	# download key file needed for ssh connection
	try:
		s3_client.download_file(bucket, sshkeyfile, '/tmp/keyfile.pem')
	except Exception as ex:
		print(f"Error getting key from S3. Make sure they exist and your bucket is in the same region as this function.")
		print(ex)
		raise ex

	# connect to EC2
	print("Connection to EC2")
	key = paramiko.RSAKey.from_private_key_file('/tmp/keyfile.pem')
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
	host = "ec2-3-22-234-128.us-east-2.compute.amazonaws.com"
	try:
		ssh.connect(hostname = host, username = "ec2-user", pkey = key)
	except Exception as ex:
		print(f"Error Connecting to EC2: {host}.")
		print(ex)
		raise ex

	# send EC2 the command to process the file
	command = f"cd AdobeProject; python3 ProcessFile.py {s3file} &"

	try:
		stdin, stdout, stderr = ssh.exec_command(command)
	except Exception as ex:
		print(f"Error executing command: {command}.")
		print(ex)
		raise ex

	out = stdout.read()
	err = stdout.read()
	if len(err):
		print(f"Command was submitted but EC2 returned an error")
		print(err)
		raise
	else:
		print(f"Successfully submitted to EC2: {command}.")
		print(f"Results: {out}")

	# the Lambda has completed it's task successfully
	return {'Status Code': 200}