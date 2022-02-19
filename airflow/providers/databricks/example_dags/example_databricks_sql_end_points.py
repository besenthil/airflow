#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This is an example DAG which uses the DatabricksSubmitRunOperator.
In this example, we create two tasks which execute sequentially.
The first task is to run a notebook at the workspace path "/test"
and the second task is to run a JAR uploaded to DBFS. Both,
tasks use new clusters.

Because we have set a downstream dependency on the notebook task,
the spark jar task will NOT run until the notebook task completes
successfully.

The definition of a successful run is if the run has a result_state of "SUCCESS".
For more information about the state of a run refer to
https://docs.databricks.com/api/latest/jobs.html#runstate
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksGetSqlEndpointsOperator, \
    DatabricksCreateSqlEndpointOperator

with DAG(
    dag_id='example_databricks_endpoint_operator',
    schedule_interval='@once',
    start_date=datetime(2022, 2, 19),
    tags=['end_point'],
    catchup=False,
) as dag:
    import os
    # databricks_connection = os.environ["AIRFLOW_CONN_DATABRICKS_DEFAULT"]
    task1 = DatabricksGetSqlEndpointsOperator(task_id='get_sql_end_points',)
    #print(task1.execute())

    json = {
        "name": "New SQL Endpoint",
        "cluster_size": "2X-Small",
        "min_num_clusters": 1,
        "max_num_clusters": 1,
        "auto_stop_mins": 10,
        "tags": [],
        "spot_instance_policy": "COST_OPTIMIZED",
        "enable_photon": False,
        "enable_serverless_compute": False,
        "channel": "CHANNEL_NAME_CURRENT"
    }

    task2 = DatabricksCreateSqlEndpointOperator(task_id='create_sql_end_points', json=json)
    #print(task2.execute())
    task1 >> task2
