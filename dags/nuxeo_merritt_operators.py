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

def extract_prefix_from_pages(pages: str):
    """
    determine the common prefix for page filepaths
    return prefix and list of pages with prefix removed
    """
    pages = json.loads(pages)
    prefix = os.path.commonprefix(pages)
    pages = [path.removeprefix(prefix) for path in pages]
    return prefix, json.dumps(pages)

class NuxeoMerrittEcsOperator(EcsRunTaskOperator):
    def __init__(self, collection_id=None, **kwargs):
        container_name = "nuxeo-merritt"

        args = {
            "cluster": "nuxeo",
            "launch_type": "FARGATE",
            "platform_version": "LATEST",
            "task_definition": "nuxeo-merritt-task-definition",
            "overrides": {
                "containerOverrides": [
                    {
                        "name": container_name,
                        "command": [
                            "--collection",
                            collection_id
                        ]
                    }
                ]
            },
            "region": "us-west-2",
            "awslogs_group": "nuxeo-merritt",
            "awslogs_region": "us-west-2",
            "awslogs_stream_prefix": "ecs/nuxeo-merritt",
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
                    target="/nuxeo-merritt",
                    type="bind"
                )
            ]
        if not mounts:
            mounts=None

        container_image = os.environ.get(
            'NUXEO_MERRITT_IMAGE',
            'public.ecr.aws/b6c7x7s4/nuxeo-merritt'
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
                "REGISTRY_BASE_URL": os.environ.get("REGISTRY_BASE_URL"),
                "NUXEO_TOKEN": os.environ.get("NUXEO_TOKEN"),
                "NUXEO_API": os.environ.get("NUXEO_API"),
                "NUXEO_MERRITT_METADATA": os.environ.get('NUXEO_MERRITT_METADATA'),
                "NUXEO_MERRITT_MEDIA_JSON": os.environ.get('NUXEO_MERRITT_MEDIA_JSON'),
                "NUXEO_MERRITT_FEEDS": os.environ.get('NUXEO_MERRITT_FEEDS'),
            },
            "max_active_tis_per_dag": 4
        }
        args.update(kwargs)
        print(f"{args=}")
        super().__init__(**args)

    def execute(self, context):
        print(f"Running {self.command} on {self.image} image")
        return super().execute(context)

if CONTAINER_EXECUTION_ENVIRONMENT == 'ecs':
    NuxeoMerrittOperator = NuxeoMerrittEcsOperator
else:
    NuxeoMerrittOperator = NuxeoMerrittDockerOperator