
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import os 
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)

load_dotenv()

client_id = os.environ.get('client_id')
client_secret = os.environ.get('client_secret')
rg_name = os.environ.get('rg_name')
adf_name = os.environ.get('adf_name')
subscription_id = os.environ.get('subscription_id')
tenant_id = os.environ.get('tenant_id')
last_updated_days = int(os.environ.get('last_updated_days'))

# these are the costs from https://azure.microsoft.com/en-us/pricing/details/data-factory/data-pipeline/
mapping_data_pipeline = {'Type': ['DataMovement','DataMovement' ,'ExternalActivity','ExternalActivity','PipelineActivity', 'PipelineActivity','executedataflow'], 'IR': ['AzureIR', 'SelfhostedIR','AzureIR', 'SelfhostedIR','AzureIR', 'SelfhostedIR','General'], 'Costs_per_Unit': [0.225, 0.090, 0.000225, 0.000090,0.005, 0.001800, 0.242]}



def build_filter_body(last_updated_after, last_updated_before, continuationToken=None):
    """generates body for REST API to get ADF activities

        Args:
            continuationToken (string, optional): _description_. Defaults to None.

        Returns:
            json: json with keys 'continuationToken', 'LastUpdatedAfter', 'LastUpdatedBefore'
    """
    # body without timetamps and only with tokens will return an empty result
    body = {'continuationToken': continuationToken,
            'LastUpdatedAfter':  datetime.strftime(last_updated_after,"%m/%d/%Y, %H:%M:%S"),
            'LastUpdatedBefore': datetime.strftime(last_updated_before,"%m/%d/%Y, %H:%M:%S")
            }
    return body

def return_token_str(url, body):
    """returns new token for the rest api

        Args:
            url (string): _description_
            body (json): body from method 

        Returns:
            string: token from url
    """
    response = requests.get(url,data=body)
    json_data = json.loads(response.text)
    token = json_data['token_type'] + ' ' +  json_data['access_token']
    return token
    
def build_authentication_body(client_id, client_secret):
    """builds body to get token to query data from rest api

        Args:
            client_id (string): Azure client id from service principal assigned to used adf
            client_secret (string): Azure client secret from service principal assigned to used adf

        Returns:
            json: json with keys grant_type, client_id, client_secret, resource
    """
    body = {'grant_type':'client_credentials',
        'client_id':client_id,
        'client_secret':client_secret,
        'resource':'https://management.core.windows.net/'}
    return body 

def get_activity_runs_by_pipeline_run(subscription_id, rg_name, adf_name, pipeline_id,token, body):
    """returns activities per pipeline run

        Args:
            subscription_id (string): Azure subsription id
            rg_name (string): Azure ressource group name
            adf_name (string): Azure data factory name
            pipeline_id (string): pipeline id
            token (string): rest API token
            body (json): body for management.azure.com

        Returns:
            list: list with all activities per pipeline run
    """
    # rest api is used because the normal python api didn't work as expected with continuation_token and date filter
    url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{rg_name}/providers/Microsoft.DataFactory/factories/{adf_name}/pipelineruns/{pipeline_id}/queryActivityruns?api-version=2018-06-01"
    response = requests.post(url,json=body,headers={"Authorization": token})
    json_data = json.loads(response.text)
    return json_data['value']


def get_pipeline_runs(client_id, client_secret, tenant_id, last_updated_days, rg_name, adf_name):
    """returns activities per pipeline run

        Args:
            client_id (string): Azure client id
            client_secret (string): Azure client secret
            tenant_id (string): Azure tenant id
            last_updated_days (int) : number of days to get the data from
            rg_name (string): Azure ressource group name
            adf_name (string): Azure data factory name

        Returns:
            list: list with all activities per pipeline run
    """
    credentials = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    filter_params = RunFilterParameters(last_updated_after=datetime.now() - timedelta(last_updated_days), last_updated_before=datetime.now() + timedelta(1))
    filter = build_filter_body(datetime.now() - timedelta(last_updated_days), last_updated_before=datetime.now() + timedelta(1))

    pipeline_runs = adf_client.pipeline_runs.query_by_factory(resource_group_name=rg_name, factory_name=adf_name, filter_parameters = filter_params)

    data = []

    # pipelines runs only have a limited number of activity runs but returns a continuation token as long as more records exists
    while (pipeline_runs.continuation_token):

        pipeline_runs = adf_client.pipeline_runs.query_by_factory(resource_group_name=rg_name, factory_name=adf_name, filter_parameters=filter_params)
        for pipeline_run in pipeline_runs.value:
            logging.info('Run_id:' + str(pipeline_run.run_id))
            # regenerate new token for every pipeline run because token has only a limited lifetime
            token = return_token_str(f'https://login.microsoftonline.com/{tenant_id}/oauth2/token', build_authentication_body(client_id, client_secret))

            for activity_run in get_activity_runs_by_pipeline_run(subscription_id, rg_name, adf_name, pipeline_run.run_id,token, filter):
                # not every activity run in ADF cause costs but we only want to collect the runs with costs
                try:      
                    if 'billingReference' in activity_run['output'].keys():
                        rows = [pipeline_run.run_id, pipeline_run.pipeline_name, activity_run['activityName'], activity_run['activityRunId'], activity_run['status'], activity_run['activityRunStart'], activity_run['activityRunEnd'], activity_run['output']['billingReference']['billableDuration'][0]['meterType'],
                                activity_run['output']['billingReference']['billableDuration'][0]['duration'], activity_run['output']['billingReference']['activityType'],activity_run['output']['billingReference']['billableDuration'][0]['unit']]
                        data.append(rows)
                except:
                    continue
            
        filter_params = RunFilterParameters(last_updated_after=datetime.now() - timedelta(last_updated_days), last_updated_before=datetime.now() + timedelta(1), continuation_token=pipeline_runs.continuation_token)
        
    return data

def get_adf_costs():
    """writes ADF runs with costs to .csv file
    """
    
    data = get_pipeline_runs(client_id, client_secret, tenant_id, last_updated_days, rg_name, adf_name)
    header = ['Run_id','Pipeline_name', 'activity_name', 'activity_run_id', 'status', 'activity_run_start', 'activity_run_end', 'meter_type', 'duration','activity_type','unit']

    df_mapping_data_pipeline = pd.DataFrame.from_dict(mapping_data_pipeline)

        
    df = pd.DataFrame(data, columns=header)
    df = df.merge(df_mapping_data_pipeline, how='left', left_on=['activity_type', 'meter_type'], right_on=['Type', 'IR'])
    df['costs_per_activity'] = df['duration'] * df['Costs_per_Unit']
    df.to_csv('ADF_costs.csv',index=False,decimal=',')
    
if __name__ == "__main__":
    get_adf_costs()
            
            