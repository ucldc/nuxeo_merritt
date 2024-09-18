import argparse

import boto3

def main(args):
    command = []
    if args.all:
        command = ["--all"]
        campus = "all"
    else:
        command = ["--collection", args.collection]
        collection = args.collection
    if args.version:
        command.extend(["--version", args.version])

    # pad-dsc-admin account
    # cluster = "nuxeo"
    # subnets = ["subnet-b07689e9", "subnet-ee63cf99"] # Public subnets in the nuxeo VPC
    # security_groups = ["sg-51064f34"] # default security group for nuxeo VPC

    # pad-prd-admin account
    cluster = "nuxeo"
    subnets = ["subnet-099bfa1a90f5296f2", "subnet-03516bc69ea5c9775"] # Public subnets in the cdl-dsc-vpc VPC
    security_groups = ["sg-0093f8807728ca7f1"] # cdl-dsc-vpc-sg security group
    
    ecs_client = boto3.client("ecs")
    response = ecs_client.run_task(
        cluster = cluster,
        capacityProviderStrategy=[
            {
                "capacityProvider": "FARGATE",
                "weight": 1,
                "base": 1
            },
        ],
        taskDefinition = "nuxeo_merritt-task-definition",
        count = 1,
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": subnets,
                "securityGroups": security_groups,
                "assignPublicIp": "ENABLED"
            }
        },
        platformVersion="LATEST",
        overrides = {
            "containerOverrides": [
                {
                    "name": "nuxeo_merritt",
                    "command": command
                }
            ]
        },
        tags=[
            {
                "key": "collection",
                "value": collection
            }
        ],
        enableECSManagedTags=True,
        enableExecuteCommand=True
    )
    task_arn = [task['taskArn'] for task in response['tasks']][0]
    waiter = ecs_client.get_waiter('tasks_stopped')
    print(f"Started task in `{cluster}` cluster: {task_arn}")
    print(f"Waiting until task has stopped...")
    try:
        waiter.wait(
            cluster = cluster,
            tasks = [task_arn],
            WaiterConfig = {
                'Delay': 10,
                'MaxAttempts': 120
            }
        )
    except Exception as e:
        print('Task failed to finish running.', e)
    else:
        print('Task finished running.')

    response = ecs_client.describe_tasks(
        cluster = cluster,
        tasks = [task_arn],
        include = ['TAGS']
    )

    # import pprint
    # pprint.pp(response)
    for task in response['tasks']:
        for container in task['containers']:
            exit_code = container.get('exitCode')
            if exit_code != 0:
                print(f"ERROR: {container['name']} had non-zero exit code: {exit_code}")

    print("View python output in CloudWatch. Log group is named `nuxeo-component-ordering`.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create Nuxeo atom feed(s) for Merritt')
    top_folder = parser.add_mutually_exclusive_group(required=True)
    top_folder.add_argument('--all', help='Create all feeds', action='store_true')
    top_folder.add_argument('--collection', help='single collection ID')
    parser.add_argument('--version', help='Metadata version. If provided, metadata will be fetched from S3.')

    args = parser.parse_args()
    (main(args))