import boto3
import json
import logging
import sys
import time
import db
import config as cfg
import os
import utils

REGION = os.environ["AWS_REGION"]


FAILED_EXIT_CODE = 1

def getAccountID():
    client = boto3.client("sts")
    response = client.get_caller_identity()["Account"]
    return response
accnt_id = getAccountID()

datasync_policy_json = {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "datasync.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

# Enable the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# Connect to AWS boto3 Client
def aws_connect_client(service):
    try:
        # Gaining API session
        #session = boto3.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        session = boto3.Session()
        my_session = boto3.session.Session()
        REGION = my_session.region_name
        # Connect the resource
        conn_client = session.client(service, REGION)
    except Exception as e:
        logger.error('Could not connect to region: %s and resources: %s , Exception: %s\n' % (REGION, service, e))
        conn_client = None
    return conn_client, REGION
datasyncclient, _ = aws_connect_client("datasync")
def getDataSyncRolePolicy(bucket):
    policy_json = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads"
                ],
                "Effect": "Allow",
                "Resource": f"arn:aws:s3:::{bucket}"
            },
            {
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:DeleteObject",
                    "s3:GetObject",
                    "s3:ListMultipartUploadParts",
                    "s3:PutObjectTagging",
                    "s3:GetObjectTagging",
                    "s3:PutObject"
                ],
                "Effect": "Allow",
                "Resource": f"arn:aws:s3:::{bucket}/*"
            }
        ]
    }
    return policy_json



def createPolicy(bucket):
    conn, _ = aws_connect_client("iam")
    try:
        policy_list = conn.list_policies()
        policyname_list = [i['PolicyName'] for i in policy_list['Policies']]
        policy_json = getDataSyncRolePolicy(bucket)
        policy_name = f"AWSDataSyncS3BucketAccess-{bucket}"
        if policy_name in policyname_list:
            policyARNlist = [i['Arn'] for i in policy_list['Policies'] if policy_name in i['PolicyName']]
            policyARN=policyARNlist[0]
            logger.info(f"The policy is already present : {policyARN}")
        else:
            response = conn.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_json)
            )
            logger.info(f"The policy has been created. Name: {policy_name}")
            policyARN=response['Policy']['Arn']
    except Exception as err:
        logger.error(f"Unable to create a policy Exception: {err}")
        sys.exit(FAILED_EXIT_CODE)
    return policyARN


def AttachRole(bucket):
    conn, _ = aws_connect_client("iam")
    role_name = "datasync-role-source"
    resource = boto3.resource('iam')
    
    roleArn = "arn:aws:iam::"+accnt_id+":role/"+role_name
    try: 
        policyARN = createPolicy(bucket)
        role = resource.Role(role_name)
        role.attach_policy(
            PolicyArn=policyARN
        )

        logger.info("Attached policy into the role..."+role_name) 
    
    except Exception as err:
        logger.error(f"Unable to create a role, Exception: {err}")
        sys.exit(FAILED_EXIT_CODE)
    return roleArn


def createDSLocation(bucket,prefix):
    
    s3arn = "arn:aws:s3:::"+bucket
    roleARN = AttachRole(bucket)
    try:
        datasync_loc = datasyncclient.create_location_s3(
                S3BucketArn=f"{s3arn}", S3StorageClass="STANDARD",
                Subdirectory=prefix,
                S3Config={"BucketAccessRoleArn": roleARN},
        )
        logger.info("Location ARN :")
        logger.info(datasync_loc)
        if datasync_loc['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info("Datasync location is created...")
            return datasync_loc["LocationArn"]
    except Exception as err:
        logger.error(f"Unable to datasync location, Exception: {err}")
        sys.exit(FAILED_EXIT_CODE)

def create_task(src, dest, execution_id,product_name):
    print("Creating a Task : source: " + src)
    print("Creating a Task : dest bucket=" + dest)
    try:
        prefix = "/output"

        SourceLocationArn=createDSLocation(src,prefix)
        DestinationLocationArn=createDSLocation(dest,prefix)
        TASKNAME="task_"+execution_id
        options = {
            "VerifyMode": "ONLY_FILES_TRANSFERRED",
            "Atime": "BEST_EFFORT",
            "Mtime": "PRESERVE",
            "TaskQueueing": "ENABLED",
            "LogLevel": "BASIC",
            "TransferMode": "CHANGED",
        }

        response = datasyncclient.create_task(
            SourceLocationArn=SourceLocationArn,
            DestinationLocationArn=DestinationLocationArn,
            CloudWatchLogGroupArn="arn:aws:logs:"
            + REGION
            + ":"
            + accnt_id
            + ":log-group:/aws/datasync",
            Name=TASKNAME,
            Options=options
        )
        task_arn = response["TaskArn"]
        print("Task ARN:" + task_arn)
        return task_arn
    except Exception as e:
        print(f"Missing parameters. Exception : {e}")
        raise e



def handler(event, context):
    logger.info("------------------")
    logger.info(event)
    try:
        logger.info("------------------")

        SourceS3Bucket = "SourceBucketName"
        DestinationS3Bucket = external_bucket
        try: 
            task_arn=create_task(SourceS3Bucket,DestinationS3Bucket,execution_id,product_name)
        except Exception as err:
            print(f"Unable to create Datasync task for external data copy : Exception: {err}")
            raise err
        try:
            response = datasyncclient.start_task_execution(TaskArn=task_arn)
            task_execution_arn = response["TaskExecutionArn"]
            print("start_exec: " + task_execution_arn)
            time.sleep(30)
            response = datasyncclient.describe_task_execution(
                TaskExecutionArn=task_execution_arn
            )
            print(response)
            if response["Status"] == "ERROR":
                return {"execution_id": execution_id, "cluster_id" : cluster_id, "cluster_name" : cluster_name,"status": "ERROR","task_id":"0"}
            else:
                return {"execution_id": execution_id, "cluster_id" : cluster_id, "cluster_name" : cluster_name,"status": response["Status"],"task_id":task_execution_arn}
        except Exception as err:
            print(f"Unable to start task for external data copy : Exception: {err}")
            return {"execution_id": execution_id, "cluster_id" : cluster_id, "cluster_name" : cluster_name,"status": "FAILED","task_id":"0"}

    except Exception as err:
        logger.error(f"Other error occurred: {err}")
        return {"execution_id": execution_id, "cluster_id" : cluster_id, "cluster_name" : cluster_name,"status": "FAILED","task_id":"0"}
