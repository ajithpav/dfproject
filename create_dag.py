import airflow
import logging
from airflow.models import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
import json
import airflow.settings
from airflow.models import DagModel
import os
import uuid
import pendulum
from datetime import datetime

local_tz = pendulum.timezone("Asia/Kolkata")
"""
Create Json files in dags/dags_json folder
This dag is Manual trigger.
"""
default_args = {
    'owner': 'Jeyakumar.C',
    'start_date': datetime(2016, 1, 1, tzinfo=local_tz),
}


def pause_or_resume(dag_id, is_paused):
    """
    A way to programatically unpause a DAG.
    :param dag_id: dag_id
    :parm is_paused: bool
    :return: dag.is_paused is now False
    """
    session = airflow.settings.Session()
    try:
        qry = session.query(DagModel).filter(DagModel.dag_id == dag_id)
        d = qry.first()
        d.is_paused = is_paused
        session.commit()
    except:
        print("Dag Id not exists")
        session.rollback()
    finally:
        session.close()

def parse_tasks(task):
    for t in task:
        t.update({"task_id":uuid.uuid4().hex})
    for i in range(len(task)):
        if i == len(task)-1:
            task[i].update({"next_task_id":''})
        else:
            task[i].update({"next_task_id": task[i+1]["task_id"]})
        if i == 0:
            task[i].update({"previous_task_id":''})
        else:
            task[i].update({"previous_task_id": task[i-1]["task_id"]})
    return task

def move_custom_query_to_filters(params):
    print(params["conf"]["tasks"])
    source_task = params["conf"]["tasks"][0]["filters"]
    custom_query = params["conf"]["tasks"][0]["analysis_custom_query"]
    source_task["analysis_custom_query"] = custom_query
    return params

def create_json_dags(**kwargs):
    params = move_custom_query_to_filters(kwargs['params'])
    file_name = None
    json_object = None
    dag_id = None
    dag_status = 'InActive'
    print(kwargs)
    if "data_map_id" in params:
        data_map_id = params['data_map_id']
        file_name = "opexwise_" + str(data_map_id) + ".json"
        if "conf" in params:
            json_conf = params['conf']
            json_conf["tasks"] = parse_tasks(json_conf["tasks"])
            dag_id = json_conf['dag_id']
            json_conf['data_map_id'] = data_map_id
            dag_status = json_conf.get("dag_status")
            if dag_status:
                if dag_status == "DRAFT":
                    logging.info("skipping the draft dag")
                    raise AirflowSkipException
            else:
                dag_status = "InActive"
            json_object = json.dumps(json_conf, indent=4)
            extra_ml_details = json_conf.get("ml_algorithm_details")
            kwargs["ti"].xcom_push(
            key="{}_extra_params".format(json_conf["dag_id"]), value=extra_ml_details)

    dag_is_paused = False if dag_status.upper() == 'ACTIVE' else True
    if dag_id is not None and file_name is not None and json_object is not None: 
        current_dir = os.path.dirname(os.path.realpath(__file__))
        dag_json_file_path = os.path.join(current_dir,"dags_json",file_name)
        # pause or unpause dag
        if os.path.exists(dag_json_file_path):
            pause_or_resume(dag_id, dag_is_paused)

        with open(dag_json_file_path, "w") as outfile:
            outfile.write(json_object)
    else:
        print(dag_id,file_name,json_object)
        raise ValueError("Invalid Requests.. Please check data_map_id and conf")


dag = DAG(dag_id='create_dag', default_args=default_args, schedule_interval=None, tags=['opexwise', "api"])
dag.doc_md = """
            #### Create Dag
            Opexwise will trigger to this dag to create json file. The json file are placed into the folder (dags/dag_json)
            """
with dag:
    json_dag = PythonOperator(
        task_id="json_dag",
        provide_context=True,
        python_callable=create_json_dags,
        dag=dag
    )
