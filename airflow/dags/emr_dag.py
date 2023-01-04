import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from datetime import datetime

SPARK_STEPS = [
    {
        "Name": "ETL",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", "s3a://doezaza-s3-endpoint/etl_scripts/etl.py"],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    "Name": "ETL_multiplatform_apps_data",
    "ReleaseLabel": "emr-5.36.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Worker node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "Steps": SPARK_STEPS,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


with DAG(
    dag_id=os.path.splitext(__file__)[0].split('/')[-1],
    default_args={
        'retries': 0
    },
    description='Spawn_EMR_Job',
    schedule=None,
    start_date=datetime.now(),
    catchup=False
) as dag:
    start = EmptyOperator(
        task_id='start'
    )

    create_job_flow = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        aws_conn_id='AWS_CREDENTIALS',
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    end = EmptyOperator(
        task_id='end'
    )

    start >> create_job_flow >> end
