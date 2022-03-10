# datasync

## nfs_to_s3_copy.py
This lambda performs following steps to transfer on-premise data to S3

1. Set up a DataSync source location (NFS server) on the destination account.

2. Set up a DataSync destination location (S3) on the destination account.

3. Create a DataSync task to initiate data transfer with specified parameters for source location, destination location, settings and task logging

4. Start execution of DataSync Task

## Lambda Input:
```json
{
	"sourceLocation": “NAME OF SOURCE Directory”,
	“destinationLocation": “NAME OF DESTINATION BUCKET”,
	“AgnetARN”: "arn:aws:datasync:<REGION>:<ACCNT ID>:agent/<AGENT ID>",
	“NFSServer”: “ServerHostname”
}
```
Note: NFSServer is the name of the NFS server. This value is the IP address or Domain Name Service (DNS) name of the NFS server. An agent that is installed on-premises uses this host name to mount the NFS server in a network.

## Lambda Output:
```json
{
    "status": “TRANSFERRING”,
    “taskid": “arn:aws:datasync:region:account-id:task/task-id”
}
```  
## s3_to_s3_copy.py
This lambda performs following steps to transfer s3 data to another account S3

1. Create a custom DataSync policy for specific source and destination buckets, (replace <sourcebucket> and <destinationbucket> with appropriate values).
```json
  "Version": "2012-10-17",
        "Statement": [
            {
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads"
                ],
                "Effect": "Allow",
                "Resource": 
                      "arn:aws:s3:::<sourcebucket>",
                      "arn:aws:s3:::<destinationbucket>"
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
                  "Resource": 
                        "arn:aws:s3:::<sourcebucket>/*",
                        "arn:aws:s3:::<destinationbucket>/*"
              }
          ]
      } 
```
2. Attach the custom policy above to datasync-role-source role.<br />
3. Set up a DataSync destination location (S3) on the destination account.<br />
4. Set up a DataSync source location (S3) on the destination account.<br />
5. Create a DataSync task to initiate data transfer with specified parameters for source location, destination location, settings and task logging.<br />
6. Start execution of DataSync Task.<br />

## Lambda Input:

```json
{
	"SourceBucketName": “NAME OF SOURCE BUCKET”,

	“external_bucket": “NAME OF DESTINATION BUCKET”

}
```
## Lambda Output:

```json
{
	"status": “TRANSFERRING”,

	“taskid": “arn:aws:datasync:region:account-id:task/task-id”

}
```
