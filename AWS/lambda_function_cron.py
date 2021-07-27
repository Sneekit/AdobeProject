# This Lambda runs every 4 hours as a scheduled event from Cloud Watch, and will attempt to process any files that failed
# It uses an S3 Tag to track the retry attempts

import json
import boto3
from datetime import datetime, timezone

MAXATTEMPTS = 3
S3CLIENT = boto3.client("s3")

def get_attempts(s3object):
	'''
		Reads the Retry Attempts tag from the s3 object
	'''

	attempts = 0
	tags = S3CLIENT.get_object_tagging(Bucket = "adobe-project", Key = s3object.key)
	try:
		# locate number of attempts
		for tag in tags['TagSet']:
			if tag['Key'] == "Retry Attempts":
				attempts = int(tag['Value'])
	except:
		pass

	return attempts

def set_attempts(s3object, attempts):
	'''
		Sets the Retry Attempts tag from the s3 object
	'''

	S3CLIENT.put_object_tagging(
		Bucket = "adobe-project",
		Key = s3object.key,    
		Tagging = {
			"TagSet": [
				{
					"Key": "Retry Attempts",
					"Value": f"{attempts}"
				},
			]
		}
	)

def resubmit_lambda(s3object):
	"""
		Resubmits the EC2 file processing lambda
	"""

	# creates payload for next Lambda event
	payload = {
		"Records": [
			{
				"s3": {
					"bucket": {
						"name": "adobe-project",
					},
					"object": {
						"key": s3object.key
					}
				}
			}
		]
	}

	# submit the lambda that processes the file in EC2
	lambdaclient = boto3.client("lambda")
	try:
		lambdaclient.invoke(
			FunctionName = "adobe-s3-get-function",
			InvocationType = "Event",
			Payload = bytes(json.dumps(payload), encoding = "utf8")
		)
		print(f"Triggerd lambda for {s3object.key}")
	except Exception as ex:
		print(f"Failed to trigger lambda for {s3object.key} {ex}.")

def lambda_handler(event, context):
	s3 = boto3.resource("s3")
	bucket = s3.Bucket("adobe-project")
	subfolder = "inbound/"
	
	for s3object in bucket.objects.all():
		if s3object.key.startswith(subfolder) and s3object.key != subfolder:
			# get tags for S3 object
			attempts  = get_attempts(s3object)
			if attempts >= MAXATTEMPTS:
				# TODO: send notification to client letting them know that this file will not process
				continue
			
			# allow one attempt every 4 hours as long as the last modified date is (attempts * 4) hours in the past
			difference = s3object.last_modified - datetime.now(timezone.utc)
			if difference.seconds / 60 / 60 >= (4 * (attempts + 1)):
				print(f"Processing file: {s3object.key}, Attempt: {attempts + 1}")
				resubmit_lambda(s3object)
				set_attempts(s3object, attempts + 1)		
			
	return {"Status Code": 200}
