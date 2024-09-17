from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from nuxeo_merritt.dags.nuxeo_merritt_operators import NuxeoMerrittOperator
import nuxeo_merritt.nuxeo_merritt as create_atom

@task(task_id="get_collection_ids")
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
    #on_failure_callback=notify_dag_failure,
    #on_success_callback=notify_dag_success,
)
def nuxeo_merritt():
    collection_ids = get_collection_ids_task()
    run_nuxeo_merritt_in_ecs_task = (
        NuxeoMerrittOperator
            .partial(
                task_id="nuxeo_merritt"
            )
            .expand(
                collection_id=collection_ids
            )
    )
    run_nuxeo_merritt_in_ecs_task.set_upstream(collection_ids)
nuxeo_merritt()