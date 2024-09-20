from datetime import datetime
import json
import os
import traceback
import urllib3

from airflow.decorators import dag, task
from airflow.models.param import Param
from nuxeo_merritt.dags.nuxeo_merritt_operators import NuxeoMerrittOperator
import nuxeo_merritt.nuxeo_merritt as create_atom

def send_message_to_slack(context: dict, task_message: dict):
    base_url = context['conf'].get('webserver', 'BASE_URL')
    dag_run = context['dag_run']
    log_message = {
        'dag_id': dag_run.dag_id,
        'dag_run_id': dag_run.run_id,
        'logical_date': dag_run.logical_date.isoformat(),
        'dag_run_conf': dag_run.conf,
        'host': base_url
    }
    task_instance = context.get('task_instance')
    if task_instance:
        log_message.update({
            'task_id': task_instance.task_id,
            'try_number': task_instance.prev_attempted_tries,
            'map_index': task_instance.map_index,
        })
    log_message['nuxeo_merritt_message'] = task_message
    message_body = json.dumps(log_message)
    slack_url = os.environ.get("NUXEO_MERRITT_SLACK_URL")
    data = {
        "channel": "#nuxeo_merritt_log",
        "username": "NuxeoMerritt",
        "text": message_body
    }
    http = urllib3.PoolManager()
    # resp = http.request(
    #               "POST",
    #               slack_url,
    #               body=json.dumps(data).encode("utf-8")
    #           )

def notify_nuxeo_merritt_success(context: dict, message: dict):
    send_message_to_slack(context, {'nuxeo_merritt_success': True})

def notify_nuxeo_merritt_failure(context: dict):
    exception = context['exception']
    tb_as_str_list = traceback.format_exception(
        type(exception), exception, exception.__traceback__)
    exc_as_str_list = traceback.format_exception_only(
        type(exception), exception)

    message = {
        'error': True,
        'exception': '\n'.join(exc_as_str_list),
        'traceback': ''.join(tb_as_str_list)
    }
    send_message_to_slack(context, message)

def notify_dag_success(context):
    message = {'dag_complete': True}
    send_message_to_slack(context, message)

def notify_dag_failure(context):
    message = {
        'dag_complete': False,
        'error': True,
        'reason': context.get('reason', 'Unknown reason')
    }
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
    schedule=None,
    start_date=datetime(2023, 1, 1),
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
                on_failure_callback=notify_nuxeo_merritt_failure,
                on_success_callback=notify_nuxeo_merritt_success
            )
            .expand(
                collection_id=collection_ids
            )
    )
    run_nuxeo_merritt_in_ecs_task.set_upstream(collection_ids)
    # send a message to SNS with summary
nuxeo_merritt()