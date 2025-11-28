import boto3
import json
from datetime import datetime
import configparser

config = configparser.ConfigParser()
config.read("awsconfig.ini")

emr = boto3.client("emr", verify=False)

def fetch_step_metadata(keyword):

    all_steps = []
    # -------------------------------
    # Paginator for list_clusters
    # -------------------------------
    cluster_paginator = emr.get_paginator("list_clusters")
    for cluster_page in cluster_paginator.paginate(ClusterStates=['RUNNING','WAITING','TERMINATED']):
        clusters = cluster_page.get("Clusters", [])

        # -------------------------------
        # Paginator for list_steps per cluster
        # -------------------------------
        for c in clusters:
            step_paginator = emr.get_paginator("list_steps")
            for step_page in step_paginator.paginate(ClusterId=c["Id"]):
                steps = step_page.get("Steps", [])

                for step in steps:
                    if keyword.lower() in step["Name"].lower():
                        detail = emr.describe_step(ClusterId=c["Id"], StepId=step["Id"])
                        timeline = detail['Step']['Status'].get('Timeline', {})

                        required_details = {
                            "StepId": step["Id"],
                            "StepName": detail['Step']['Name'],
                            "ExecutionStatus": detail['Step']['Status']['State'],
                            "StartDateTime": timeline.get('StartDateTime').strftime("%Y-%m-%dT%H:%M:%S") if timeline.get('StartDateTime') else None,
                            "EndDateTime": timeline.get('EndDateTime').strftime("%Y-%m-%dT%H:%M:%S") if timeline.get('EndDateTime') else None,
                            "ExecutionWeekDay": timeline.get('StartDateTime').strftime("%A") if timeline.get('StartDateTime') else None
                        }

                        all_steps.append(required_details)


    return all_steps


def write_S3(keyword,steps_json):
    # Generate dynamic date path
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{keyword}/{timestamp}.json"

    # Convert dict to JSON text
    json_body = json.dumps(steps_json, indent=2)

    # Upload to S3
    s3 = boto3.client("s3", verify=False)
    bucket=config['aws']['bucket_name']
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json_body,
        ContentType="application/json"
    )
    return bucket,s3_key

def load_json_array_from_s3(bucket, key):
    s3 = boto3.client("s3", verify=False)
    response = s3.get_object(Bucket=bucket, Key=key)
    text = response["Body"].read().decode("utf-8")
    return json.loads(text)

def getAwsAccountdetails():
    sts = boto3.client("sts", verify=False)
    account_id = sts.get_caller_identity()["Account"]

    """org = boto3.client("organizations", verify=False)
    details = org.describe_account(AccountId=account_id)["Account"]"""
    return account_id

#print(getAwsAccountdetails())
#print(fetch_step_metadata("Batch1"))