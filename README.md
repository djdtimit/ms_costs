# ms_costs

- python=3.8.10
- install python packages via requirements.txt
- .env file must contain the following id's from the Service Principal registered to the ADF 
(for adf_name = 'datafactory-vr-prod' it is Service Principal 
"Az DevOps - Service Connection Datalake-vR-Prod - rundstedt-vr-data-platform-2ef279ee-006c-46ea-8b0f-e2e010c549de" 
for the rundstedt subscription):
    -client_id 
    -client_secret 
    -subscription_id 
    -tenant_id 
- ressource group name (rg_name), data factory name (adf_name) and last updated days (determines the range between the current date 
and the date in the past) are also needed
    -rg_name 
    -adf_name 
    -last_updated_days 




