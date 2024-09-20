import os
import boto3
import json

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

from docker.types import Mount

# generally speaking, the CONTAINER_EXECUTION_ENVIRONMENT should always
# be 'ecs' in deployed MWAA and should always be 'docker' in local development.
# if one is working to debug airflow's connection to ecs, though, it may be 
# useful to set CONTAINER_EXECUTION_ENVIRONMENT to 'ecs' locally. 
CONTAINER_EXECUTION_ENVIRONMENT = os.environ.get(
    'CONTAINER_EXECUTION_ENVIRONMENT')


def get_awsvpc_config():
    """ 
    get private subnets and security group from cloudformation stack for use
    with the NuxeoMerrittEcsOperator to run tasks in an ECS cluster
    """
    client = boto3.client('cloudformation', region_name='us-west-2')
    awsvpcConfig = {
        "subnets": [],
        "securityGroups": []
    }
    cf_outputs = (client
                        .describe_stacks(StackName='pad-airflow-mwaa')
                        .get('Stacks', [{}])[0]
                        .get('Outputs', [])
    )
    for output in cf_outputs:
        if output['OutputKey'] in ['PrivateSubnet1', 'PrivateSubnet2']:
            awsvpcConfig['subnets'].append(output['OutputValue'])
        if output['OutputKey'] == 'SecurityGroup':
            awsvpcConfig['securityGroups'].append(output['OutputValue'])
    return awsvpcConfig

class NuxeoMerrittEcsOperator(EcsRunTaskOperator):
    def __init__(self, collection_id=None, **kwargs):
        container_name = "nuxeo_merritt"

        args = {
            "cluster": "nuxeo",
            "launch_type": "FARGATE",
            "platform_version": "LATEST",
            "task_definition": "nuxeo_merritt-task-definition",
            "overrides": {
                "containerOverrides": [
                    {
                        "name": container_name,
                        "command": [
                            "--collection",
                            collection_id
                        ],
                        "environment": [
                            {
                                "name": "NUXEO_MERRITT_NUXEO_TOKEN",
                                "value": os.environ.get("NUXEO_MERRITT_NUXEO_TOKEN")
                            },
                            {
                                "name": "NUXEO_MERRITT_NUXEO_API",
                                "value": os.environ.get("NUXEO_MERRITT_NUXEO_API")
                            },
                            {
                                "name": "NUXEO_MERRITT_METADATA",
                                "value": os.environ.get("NUXEO_MERRITT_METADATA")
                            },
                            {
                                "name": "NUXEO_MERRITT_MEDIA_JSON",
                                "value": os.environ.get("NUXEO_MERRITT_MEDIA_JSON")
                            },
                            {
                                "name": "NUXEO_MERRITT_FEEDS",
                                "value": os.environ.get("NUXEO_MERRITT_FEEDS")
                            },
                            {
                                "name": "AWS_RETRY_MODE",
                                "value": "standard"
                            },
                            {
                                "name": "AWS_MAX_ATTEMPTS",
                                "value": "10"
                            }
                        ]
                    }
                ],
            },
            "region": "us-west-2",
            "awslogs_group": "nuxeo_merritt",
            "awslogs_region": "us-west-2",
            "awslogs_stream_prefix": "ecs/nuxeo_merritt",
            "reattach": True,
            "number_logs_exception": 100,
            "waiter_delay": 10,
            "waiter_max_attempts": 8640
        }
        args.update(kwargs)
        super().__init__(**args)

    def execute(self, context):
        # Operators are instantiated once per scheduler cycle per airflow task
        # using them, regardless of whether or not that airflow task actually
        # runs. The ContentHarvestEcsOperator is used by ecs_content_harvester
        # DAG, regardless of whether or not we have proper credentials to call
        # get_awsvpc_config(). Adding network configuration here in execute
        # rather than in initialization ensures that we only call
        # get_awsvpc_config() when the operator is actually run.
        self.network_configuration = {
            "awsvpcConfiguration": get_awsvpc_config()
        }
        return super().execute(context)


class NuxeoMerrittDockerOperator(DockerOperator):
    def __init__(self, collection_id, **kwargs):
        mounts = []
        if os.environ.get("OUTPUT_MOUNT"):
            mounts.append(Mount(
                source=os.environ.get("OUTPUT_MOUNT"),
                target="/output",
                type="bind",
            ))
        if os.environ.get("MOUNT_CODEBASE"):
            mounts = mounts + [
                Mount(
                    source=os.environ.get('MOUNT_CODEBASE'),
                    target="/nuxeo_merritt",
                    type="bind"
                )
            ]
        if not mounts:
            mounts=None

        container_image = os.environ.get(
            'NUXEO_MERRITT_IMAGE',
            'public.ecr.aws/b6c7x7s4/nuxeo/nuxeo_merritt'
        )
        container_version = os.environ.get(
            'NUXEO_MERRITT_VERSION', 'latest')
        
        container_name = f"nuxeo_merritt_{collection_id}"

        args = {
            "image": f"{container_image}:{container_version}",
            "container_name": container_name,
            "command": [
                "--collection",
                collection_id
            ],
            "network_mode": "bridge",
            "auto_remove": 'force',
            "mounts": mounts,
            "mount_tmp_dir": False,
            "environment": {
                "NUXEO_MERRITT_NUXEO_TOKEN": os.environ.get("NUXEO_MERRITT_NUXEO_TOKEN"),
                "NUXEO_MERRITT_NUXEO_API": os.environ.get("NUXEO_MERRITT_NUXEO_API"),
                "NUXEO_MERRITT_METADATA": os.environ.get('NUXEO_MERRITT_METADATA'),
                "NUXEO_MERRITT_MEDIA_JSON": os.environ.get('NUXEO_MERRITT_MEDIA_JSON'),
                "NUXEO_MERRITT_FEEDS": os.environ.get('NUXEO_MERRITT_FEEDS'),
                "AWS_ACCESS_KEY_ID": os.environ.get('AWS_ACCESS_KEY_ID'),
                "AWS_SECRET_ACCESS_KEY": os.environ.get('AWS_SECRET_ACCESS_KEY'),
                "AWS_SESSION_TOKEN": os.environ.get('AWS_SESSION_TOKEN'),
            },
            "max_active_tis_per_dag": 4
        }
        args.update(kwargs)
        super().__init__(**args)

    def execute(self, context):
        print(f"Running {self.command} on {self.image} image")
        return super().execute(context)

if CONTAINER_EXECUTION_ENVIRONMENT == 'ecs':
    NuxeoMerrittOperator = NuxeoMerrittEcsOperator
else:
    NuxeoMerrittOperator = NuxeoMerrittDockerOperator