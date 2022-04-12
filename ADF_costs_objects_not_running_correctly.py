from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)


class adf_costs:
    
    load_dotenv()
    
    """class represents azure data factory costs via methods
    
    Attributes from .env file
    ----------
    client_id : str
        Azure client id from service principal assigned to used adf
    client_secret : str
        Azure client secret from service principal assigned to used adf
    rg_name : str
        ressource group name of the used adf
    adf_name : str
        name of the used ADF
    subscription_id : str
        Azure subsription id
    tenant_id : str
        Azure tenant id from service principal assigned to used adf
    last_updated_days : int
        number of days to get the data from
    """
    
    def __init__(self):
        # get credentials from .env file
        self.client_id = os.environ.get('client_id')
        self.client_secret = os.environ.get('client_secret')
        self.rg_name = os.environ.get('rg_name')
        self.adf_name = os.environ.get('adf_name')
        self.subscription_id = os.environ.get('subscription_id')
        self.tenant_id = os.environ.get('tenant_id')
        self.last_updated_days = int(os.environ.get('last_updated_days'))
         
    def build_filter_body(self, continuationToken=None):
        """generates body for REST API to get ADF activities

        Args:
            continuationToken (string, optional): _description_. Defaults to None.

        Returns:
            json: json with keys 'continuationToken', 'LastUpdatedAfter', 'LastUpdatedBefore'
        """
        last_updated_after = datetime.now() - timedelta(self.last_updated_days)
        last_updated_before = datetime.now() + timedelta(1)
        # body without timetamps and only with tokens will return an empty result
        body = {'continuationToken': continuationToken,
                'LastUpdatedAfter':  datetime.strftime(last_updated_after,"%m/%d/%Y, %H:%M:%S"),
                'LastUpdatedBefore': datetime.strftime(last_updated_before,"%m/%d/%Y, %H:%M:%S")
                }
        return body

    def return_token_str(self, url, body):
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
        
    def build_authentication_body(self, client_id, client_secret):
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

    def get_activity_runs_by_pipeline_run(self, subscription_id, rg_name, adf_name, pipeline_id,token, body):
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
        url = f"https://management.azure.com/subscriptions/{self.subscription_id}/resourceGroups/{self.rg_name}/providers/Microsoft.DataFactory/factories/{self.adf_name}/pipelineruns/{pipeline_id}/queryActivityruns?api-version=2018-06-01"
        response = requests.post(url,json=body,headers={"Authorization": token})
        json_data = json.loads(response.text)
        # logging.info('json_data:' + str(json_data))
        return json_data['value']
    
    def connect_to_adf(self):
        """generate ADF instance

        Returns:
            DataFactoryManagementClient: instance of DataFactoryManagementClient
        """
        credentials = ClientSecretCredential(client_id = self.client_id, client_secret = self.client_secret, tenant_id = self.tenant_id)
        adf_client = DataFactoryManagementClient(credentials, self.subscription_id)
        return adf_client
    
    def build_query_filter(self):
        """generates filter to query ADF pipeline runs

        Returns:
            list: 1. item -> filter for pipeline runs, 2. item -> filter for activity runs
        """
        filter_params = RunFilterParameters(last_updated_after=datetime.now() - timedelta(self.last_updated_days), last_updated_before=datetime.now() + timedelta(1))
        filter = self.build_filter_body()
        return [filter_params, filter]
    
    def get_pipeline_runs(self):
        """returns ADF activity runs over all pipeline runs specified in time filter

        Returns:
            list: list of lists with activityName, activityRunId, status, activityRunStart, activityRunEnd, meterType, duration, activityType, unit
        """
        pipeline_runs = self.connect_to_adf().pipeline_runs.query_by_factory(resource_group_name=self.rg_name, factory_name=self.adf_name, filter_parameters = self.build_query_filter()[0])
        data = []
        # pipelines runs only have a limited number of activity runs but returns a continuation token as long as more records exists
        while (pipeline_runs.continuation_token):
            try:
                pipeline_runs = self.connect_to_adf().pipeline_runs.query_by_factory(resource_group_name=self.rg_name, factory_name=self.adf_name, filter_parameters = self.build_query_filter()[0])
                for pipeline_run in pipeline_runs.value:
                    logging.info('Run_id:' + str(pipeline_run.run_id))
                    # regenerate new token for every pipeline run because token has only a limited lifetime
                    token = self.return_token_str(f'https://login.microsoftonline.com/{self.tenant_id}/oauth2/token', self.build_authentication_body(self.client_id, self.client_secret))

                    for activity_run in self.get_activity_runs_by_pipeline_run(self.subscription_id, self.rg_name, self.adf_name, pipeline_run.run_id, token, self.build_query_filter()[1]):
                        try:      
                            # not every activity run in ADF cause costs but we only want to collect the runs with costs
                            if 'billingReference' in activity_run['output'].keys():
                            
                                rows = [pipeline_run.run_id, pipeline_run.pipeline_name, activity_run['activityName'], activity_run['activityRunId'], activity_run['status'], activity_run['activityRunStart'], activity_run['activityRunEnd'], activity_run['output']['billingReference']['billableDuration'][0]['meterType'],
                                        activity_run['output']['billingReference']['billableDuration'][0]['duration'], activity_run['output']['billingReference']['activityType'],activity_run['output']['billingReference']['billableDuration'][0]['unit']]
                                data.append(rows)
                        except:
                            logging.info(f"activity_run {activity_run['activityRunId']} has no keys")
                            continue
            except:
                logging.info(f"pipeline_run has no value")
                continue
        return data 
                
    def build_costs_df(self):
        """returns mapping DataFrame with ADF costs

        Returns:
            Pandas DataFrame: to join with activity runs to get the costs per run
        """
        # these are the costs from https://azure.microsoft.com/en-us/pricing/details/data-factory/data-pipeline/
        adf_costs = {'Type': ['DataMovement','DataMovement' ,'ExternalActivity','ExternalActivity','PipelineActivity', 'PipelineActivity','executedataflow'], 'IR': ['AzureIR', 'SelfhostedIR','AzureIR', 'SelfhostedIR','AzureIR', 'SelfhostedIR','General'], 'Costs_per_Unit': [0.225, 0.090, 0.000225, 0.000090,0.005, 0.001800, 0.242]}
        df_adf_costs = pd.DataFrame.from_dict(adf_costs)
        return df_adf_costs
    
    def get_adf_costs(self):
        """writes ADF runs with costs to .csv file
        """
        header = ['Run_id','Pipeline_name', 'activity_name', 'activity_run_id', 'status', 'activity_run_start', 'activity_run_end', 'meter_type', 'duration','activity_type','unit']
        df_adf_costs = self.build_costs_df()
        
        df = pd.DataFrame(self.get_pipeline_runs(), columns=header)
        # df.to_csv('ADF_costs.csv',index=False,decimal=',')
        df = df.merge(df_adf_costs, how='left', left_on=['activity_type', 'meter_type'], right_on=['Type', 'IR'])
        df['costs_per_activity'] = df['duration'] * df['Costs_per_Unit']
        
        df.to_csv('ADF_costs.csv',index=False,decimal=',')
        
    
if __name__ == "__main__":
    adf_costs().get_adf_costs()
            
            