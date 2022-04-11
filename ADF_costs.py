from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
import time
from datetime import datetime, timedelta
import csv
import pandas as pd
import requests
import json
from dotenv import load_dotenv
import os

client_id='15ca1b7d-9a47-42ef-9c6c-a10b63961774' 
client_secret='hpG7Q~G2MN1O.ZsiYG1l2uPsswXGl3bDBHnqm' 

def build_filter_body(last_updated_after, last_updated_before, continuationToken=None):
    body = {'continuationToken': continuationToken,
            'LastUpdatedAfter':  datetime.strftime(last_updated_after,"%m/%d/%Y, %H:%M:%S"),
            'LastUpdatedBefore': datetime.strftime(last_updated_before,"%m/%d/%Y, %H:%M:%S")
            }
    return body

def return_token_str(url, body):
    response = requests.get(url,data=body)
    json_data = json.loads(response.text)
    token = json_data['token_type'] + ' ' +  json_data['access_token']
    return token
    
def build_authentication_body(client_id, client_secret):
    body = {'grant_type':'client_credentials',
        'client_id':client_id,
        'client_secret':client_secret,
        'resource':'https://management.core.windows.net/'}
    return body 

def get_activity_runs_by_pipeline_run(subscription_id, rg_name, df_name, pipeline_id,token, body):
    url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{rg_name}/providers/Microsoft.DataFactory/factories/{df_name}/pipelineruns/{pipeline_id}/queryActivityruns?api-version=2018-06-01"
    response = requests.post(url,json=body,headers={"Authorization": token})
    json_data = json.loads(response.text)
    return json_data['value']

mapping_data_pipeline = {'Type': ['DataMovement','DataMovement' ,'ExternalActivity','ExternalActivity','PipelineActivity', 'PipelineActivity','executedataflow'], 'IR': ['AzureIR', 'SelfhostedIR','AzureIR', 'SelfhostedIR','AzureIR', 'SelfhostedIR','General'], 'Costs_per_Unit': [0.225, 0.090, 0.000225, 0.000090,0.005, 0.001800, 0.242]}


rg_name = 'datalake-vr-prod'
df_name = 'datafactory-vr-prod'

last_updated_days = 3

#Create a data factory
subscription_id = '2ef279ee-006c-46ea-8b0f-e2e010c549de'
credentials = ClientSecretCredential(client_id='15ca1b7d-9a47-42ef-9c6c-a10b63961774', client_secret='hpG7Q~G2MN1O.ZsiYG1l2uPsswXGl3bDBHnqm', tenant_id='e70223ab-7ca2-4d6f-9934-0ff59f86f6e1')
adf_client = DataFactoryManagementClient(credentials, subscription_id)

filter_params = RunFilterParameters(last_updated_after=datetime.now() - timedelta(last_updated_days), last_updated_before=datetime.now() + timedelta(1))
filter_params_act = RunFilterParameters(last_updated_after=datetime.now() - timedelta(last_updated_days), last_updated_before=datetime.now() + timedelta(1))

filter = build_filter_body(datetime.now() - timedelta(last_updated_days), last_updated_before=datetime.now() + timedelta(1))

pipeline_runs = adf_client.pipeline_runs.query_by_factory(resource_group_name=rg_name, factory_name=df_name, filter_parameters = filter_params)

header = ['Run_id','Pipeline_name', 'activity_name', 'activity_run_id', 'status', 'activity_run_start', 'activity_run_end', 'meter_type', 'duration','activity_type','unit']


data = []



while (pipeline_runs.continuation_token):

    pipeline_runs = adf_client.pipeline_runs.query_by_factory(resource_group_name=rg_name, factory_name=df_name, filter_parameters=filter_params)
    for pipeline_run in pipeline_runs.value:
        print('Run_id:', pipeline_run.run_id)
        token = return_token_str('https://login.microsoftonline.com/e70223ab-7ca2-4d6f-9934-0ff59f86f6e1/oauth2/token', build_authentication_body(client_id, client_secret))

        # for activity_run in adf_client.activity_runs.query_by_pipeline_run(rg_name, df_name, pipeline_run.run_id, filter_params_act).value:
        for activity_run in get_activity_runs_by_pipeline_run(subscription_id, rg_name, df_name, pipeline_run.run_id,token, filter):
            try:      
                if 'billingReference' in activity_run['output'].keys():
                   
                    rows = [pipeline_run.run_id, pipeline_run.pipeline_name, activity_run['activityName'], activity_run['activityRunId'], activity_run['status'], activity_run['activityRunStart'], activity_run['activityRunEnd'], activity_run['output']['billingReference']['billableDuration'][0]['meterType'],
                            activity_run['output']['billingReference']['billableDuration'][0]['duration'], activity_run['output']['billingReference']['activityType'],activity_run['output']['billingReference']['billableDuration'][0]['unit']]
                    data.append(rows)
            except:
                print(f"activity_run {activity_run['activityRunId']} has no keys")
                continue
        
    
    filter_params = RunFilterParameters(last_updated_after=datetime.now() - timedelta(last_updated_days), last_updated_before=datetime.now() + timedelta(1), continuation_token=pipeline_runs.continuation_token)

df_mapping_data_pipeline = pd.DataFrame.from_dict(mapping_data_pipeline)

            
df = pd.DataFrame(data, columns=header)
df.to_csv('ADF_costs.csv',index=False,decimal=',')
df = df.merge(df_mapping_data_pipeline, how='left', left_on=['activity_type', 'meter_type'], right_on=['Type', 'IR'])
df['costs_per_activity'] = df['duration'] * df['Costs_per_Unit']
df = df.groupby(['Run_id','Pipeline_name','activity_type','unit','meter_type','Costs_per_Unit'])['duration','costs_per_activity'].sum().reset_index()
df.loc['Total'] = pd.Series(df['costs_per_activity'].sum(), index = ['costs_per_activity'])

df.to_csv('ADF_costs_summarized.csv',index=False,decimal=',')
            
            