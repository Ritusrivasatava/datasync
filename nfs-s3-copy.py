import json
import sys
import os
import json
import boto3
import logging
from datetime import datetime
import time

REGION = os.environ["AWS_REGION"]

from botocore.config import Config

config = Config(retries=dict(max_attempts=10))
## empty commit
datasyncclient = boto3.client(
    "datasync", region_name=os.environ["AWS_REGION"], config=config
)


def getAccountID():
    client = boto3.client("sts", region_name=os.environ['AWS_REGION'])
    try:
         account_id = client.get_caller_identity()["Account"]
    except Exception as err:
        print(f"Unable to get Account ID. Exception: {err}")
         sys.exit(1)
    return account_id


#AgnetARN = "arn:aws:datasync:<REGION>:<ACCNT ID>:agent/<AGENT ID>" ## Please replace this ARN with actual DATASYNC Agent ARN
accntid = getAccountID()
S3RoleArn = "arn:aws:iam::" + accntid + ":role/s3_data_sync_access"



def create_locations(client, src, dest,AgnetARN, ServerName):
    """
    Convenience function for creating locations.
    Locations must exist before tasks can be created.
    """
    nfs_arn = None
    s3_arn = None

    print("creating nfs location : ")
    print("agent arn " + AgnetARN)
    try:
        response = datasyncclient.create_location_nfs(
            ServerHostname=ServerName,
            Subdirectory=src,
            OnPremConfig={"AgentArns": [AgnetARN]},
            MountOptions={"Version": "AUTOMATIC"},
        )
        print("nfs location")
        print(response)
        nfs_arn = response["LocationArn"]
    except Exception as err:
        print(f"Unable to create datasync source location. Exception: {err}")
        publish_message(
            "Unable to create datasync source location. Exception: " + str(err)
        )
        sys.exit(1)

    print("Creating a Task locations :")
    sourceVal = [x.strip() for x in src.split("/") if x]
    print("S3 prefix")
    prefix = sourceVal.pop()
    print(prefix)
    try:
        response = datasyncclient.create_location_s3(
            S3BucketArn="arn:aws:s3:::" + dest,
            Subdirectory=prefix,
            S3Config={"BucketAccessRoleArn": S3RoleArn},
        )
        s3_arn = response["LocationArn"]
    except Exception as err:
        print(f"Unable to create datasync deatination location. Exception: {err}")
        publish_message(
            "Unable to create datasync deatination location. Exception: " + str(err)
        )
        sys.exit(1)
    return {"nfs_arn": nfs_arn, "s3_arn": s3_arn}


def create_task(src, dest, AgnetARN, ServerName):
    print("Creating a Task : source: " + src)
    print("Creating a Task : dest bucket=" + dest)
    try:
        locations = create_locations(datasyncclient, src, dest,AgnetARN, ServerName)
        sourceVal = [x.strip() for x in src.split("/") if x]
        TASKNAME = "TASK_" + sourceVal.pop()
        print("Taskname: " + TASKNAME)

        options = {
            "VerifyMode": "ONLY_FILES_TRANSFERRED",
            "Atime": "BEST_EFFORT",
            "Mtime": "PRESERVE",
            "TaskQueueing": "ENABLED",
            "LogLevel": "BASIC",
            "TransferMode": "CHANGED",
        }
        response = datasyncclient.create_task(
            SourceLocationArn=locations["nfs_arn"],
            DestinationLocationArn=locations["s3_arn"],
            CloudWatchLogGroupArn="arn:aws:logs:"
            + REGION
            + ":"
            + accntid
            + ":log-group:/aws/datasync",
            Name=TASKNAME,
            Options=options,
            
        )
        task_arn = response["TaskArn"]
        print("Task ARN:" + task_arn)
        return task_arn
    except Exception as e:
        print(f"Missing parameters. Exception : {e}")
        publish_message("Create Task Failed, Exception: " + str(e))


def describe_task(task_arn):
    response = datasyncclient.describe_task(TaskArn=task_arn)
    return response["Status"]


def start_exec(task_name, task_arn):
    print("Starting Task execution :" + task_name)
    try:
        response = datasyncclient.start_task_execution(TaskArn=task_arn)
        task_execution_arn = response["TaskExecutionArn"]
        print("start_exec: " + task_execution_arn)
        time.sleep(30)
        response = datasyncclient.describe_task_execution(
            TaskExecutionArn=task_execution_arn
        )
        print(response)
        return response["Status"]
    except Exception as err:
        print(f"Unable to start task : {task_name}, Exception: {err}")
        if "InvalidRequestException" not in str(err):
            publish_message(
                "Unable to start task : " + task_name + ", Exception: " + str(err)
            )


def publish_message(error_msg):
    snsclient = boto3.client("sns")
    lambda_func_name = os.environ["AWS_LAMBDA_FUNCTION_NAME"]
    try:
        message = ""
        message += "\nLambda error  summary" + "\n\n"
        message += "##########################################################\n"
        message += "# LogGroup Name:- " + os.environ["AWS_LAMBDA_LOG_GROUP_NAME"] + "\n"
        message += "# LogStream:- " + os.environ["AWS_LAMBDA_LOG_STREAM_NAME"] + "\n"
        message += "# Log Message:- " + "\n"
        message += "# \t\t" + str(error_msg.split("\n")) + "\n"
        message += "##########################################################\n"
        accntid = getAccountID()
        # Sending the notification...
        snsclient.publish(
            TargetArn="arn:aws:sns:" + REGION + ":" + accntid + ":datasync-notify",
            Subject=f"Execution error for Lambda - {lambda_func_name[3]}",
            Message=message,
        )
    except Exception as e:
        print(f"Unable to publish message. Exception: {e}")


def handler(event, context):
    print(event)
    try:
        sourceLocation = event["sourceLocation"]
        destinationLocation = event["destinationLocation"]
        AgnetARN = event["AgnetARN"]
        ServerName = event["NFSServer"]
        print("source location : " + sourceLocation)
        print("dest location : " + destinationLocation)
        task_arn = create_task(sourceLocation, destinationLocation, AgnetARN, ServerName)
        wU = True
        while wU == True:
            taskStatus = describe_task(task_arn)
            print(taskStatus)
            if taskStatus == "AVAILABLE":
                wU = False
            else:
                time.sleep(30)
        status = start_exec(task_name, task_arn)
        return {"status": status, "taskid": task_arn}
                
    except Exception as err:
        print(f"Unable to execute the function. Exception : {err}")
        return {"status": "FAILED", "taskid": "0"}
        publish_message("Error in copy scheduler: " + str(err))
        raise err
        sys.exit(1)

if __name__ == "__main()__":
    handler(None, None)
