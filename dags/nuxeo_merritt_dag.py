from datetime import datetime
import json
import os
import requests
import traceback

from airflow.decorators import dag, task
from airflow.models.param import Param
from nuxeo_merritt.dags.nuxeo_merritt_operators import NuxeoMerrittOperator
import nuxeo_merritt.nuxeo_merritt as create_atom

def send_message_to_slack(context: dict, message: str):
    slack_url = os.environ.get("NUXEO_MERRITT_SLACK_URL")
    data = {
        "channel": "#nuxeo_merritt_log",
        "username": "NuxeoMerritt",
        "text": message
    }
    response = requests.post(
        url = slack_url,
        data = json.dumps(data).encode("utf-8")
    )
    response.raise_for_status()

def get_dag_run(context: dict):
    base_url = context['conf'].get('webserver', 'BASE_URL')
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    dag_run_id = dag_run.run_id
    dag_run_info = {
        'dag_run_permalink': f"{base_url}/dags/{dag_id}/grid?dag_run_id={dag_run_id}"
    }
    task_instance = context.get('task_instance')
    if task_instance:
        dag_run_info.update({
            'task_run_permalink': (
                f"{base_url}/dags/{dag_id}/grid"
                f"?dag_run_id={dag_run_id}"
                f"&task_id={task_instance.task_id}"
                f"&tab=mapped_tasks"
                f"&map_index={task_instance.map_index}"
            )
        })

    return dag_run_info

def notify_nuxeo_merritt_failure(context: dict):
    exception = context['exception']
    tb_as_str_list = traceback.format_exception(
        type(exception), exception, exception.__traceback__)
    traceback_str = ''.join(tb_as_str_list)
    exc_as_str_list = traceback.format_exception_only(
        type(exception), exception)
    exception_str = '\n'.join(exc_as_str_list)

    dag_run = get_dag_run(context)
    message = (
        f":red_circle: Nuxeo Merritt ATOM feed creation failed :red_circle:\n"
        f"*Airflow DAG Run*: {dag_run['dag_run_permalink']}\n"
        f"*Airflow Task Run*: {dag_run['task_run_permalink']}\n"
        f"*Exception*: {exception_str}\n"
        f"*Traceback*: {traceback_str}\n"
    )

    send_message_to_slack(context, message)

def notify_dag_success(context):
    dag_run = get_dag_run(context)
    message = (
        f":large_green_circle: Nuxeo Merritt ATOM feed creation finished :large_green_circle:\n"
        f"*Airflow DAG Run*: {dag_run['dag_run_permalink']}\n"
        f"*Feed Location*: {os.environ.get('NUXEO_MERRITT_FEEDS')}"
    )
    send_message_to_slack(context, message)

def notify_dag_failure(context):
    dag_run = get_dag_run(context)
    message = (
        f":red_circle: Nuxeo Merritt DAG run failed :red_circle:\n"
        f"*Airflow DAG Run*: {dag_run['dag_run_permalink']}\n"
        f"*Reason*: {context.get('reason', 'Unknown reason')}\n"
    )
    send_message_to_slack(context, message)

@task(task_id="get_collection_ids",
      on_failure_callback=notify_nuxeo_merritt_failure)
def get_collection_ids_task(params=None, **context):
    collection_id = params.get('collection_id')
    if collection_id:
        collection_ids = [collection_id]
    else:
        collection_ids = [collection['collection_id'] for collection in create_atom.get_registry_merritt_collections()]

    return collection_ids

@dag(
    dag_id="nuxeo_merritt",
    schedule='1 8 * * 6', # Saturdays at 08:01 UTC
    start_date=datetime(2024, 9, 21, 8, 6),
    catchup=False,
    params={
        'collection_id': Param(
            None, 
            description="Optional Collection ID for which to create ATOM feed. If none provided, all feeds will be created."
        ),
    },
    tags=["nuxeo"],
    on_failure_callback=notify_dag_failure,
    on_success_callback=notify_dag_success,
)
def nuxeo_merritt():
    collection_ids = get_collection_ids_task()
    run_nuxeo_merritt_in_ecs_task = (
        NuxeoMerrittOperator
            .partial(
                task_id="create_feeds",
                on_failure_callback=notify_nuxeo_merritt_failure
            )
            .expand(
                collection_id=collection_ids
            )
    )
    run_nuxeo_merritt_in_ecs_task.set_upstream(collection_ids)
nuxeo_merritt()