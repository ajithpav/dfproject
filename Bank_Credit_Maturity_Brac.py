from datetime import timedelta
from datetime import datetime
import json
import os
import re
import shutil
import pandas as pd
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
import numpy as np
import requests
import common
from dfenum import DFConstant,Country

init_session = None
jsondata = common.getdfconfigdetails()
err_to_emails = jsondata['err_log_emails']
GRAPH_API_ENDPOINT = jsondata['graphAPI_URL']
client_id=jsondata['client_id']
tenent_id=jsondata['tenent_id']
email_text = 'Error in Bank Credit Utilization City Airflow'
webapi_base_path = jsondata['webapi_base_path']
country_name = jsondata['country']

def process_credit_maturity_data_Brac_Bank():
    try:
        print("===================================")
        starttime = datetime.today()
        session = common.get_session(init_session, jsondata)
        country_name = Country.Bangladesh.value
        filepathdetails = common.getfilepathdetails(session, jsondata, country_name)
        file_download_path = filepathdetails[0]["file_download_path"]
        processed_files_path = filepathdetails[0]["processed_files_path"]
        error_files_path = filepathdetails[0]["error_files_path"]
        email_folder = DFConstant.Brac_BANK_Report_EmailFolder.value
        credit_file_names_list = common.extractemail(session, filepathdetails, jsondata, client_id, tenent_id, email_folder, email_text, err_to_emails, GRAPH_API_ENDPOINT)
        if len(credit_file_names_list) > 0:
            for file_name in credit_file_names_list:
                e_file_path = file_download_path + file_name
                bank_name = DFConstant.Brac_bank_name.value
                
                # Process Credit Data
                try:
                    credit_data = pd.read_excel(e_file_path, engine='openpyxl', sheet_name=DFConstant.Brac_credit_sheet_name.value, dtype=str)
                except ValueError:
                    credit_data = pd.read_excel(e_file_path, engine='openpyxl', sheet_name=DFConstant.Brac_credit_sheet_name.value.replace('Template', '').strip(), dtype=str)
                credit_data = credit_data.dropna(how='all', axis='rows').replace(np.nan, '', regex=True)
                credit_data.columns = credit_data.columns.str.strip()  # Remove leading and trailing spaces from column name
                api_endpoint_url_credit = f'{webapi_base_path}/dfwebapi/process_bank_credit_Common'
                credit_data_json = credit_data.to_json(orient='records')
                credit_payload = {
                    "user_name": DFConstant.AdminUser.value,
                    "data": credit_data_json,
                    "file_name": file_name,
                    "file_path": e_file_path,
                    "bank_name": bank_name,
                    "country": country_name
                }
                credit_response = requests.post(api_endpoint_url_credit, json=credit_payload)
                credit_att_status = credit_response.json().get('att_status')
                print("Credit Status:", credit_att_status)
                

                # Process Maturity Data
                try:
                    maturity_data = pd.read_excel(e_file_path, engine='openpyxl', sheet_name=DFConstant.Brac_maturity_sheet_name.value, dtype=str)
                except ValueError:
                    maturity_data = pd.read_excel(e_file_path, engine='openpyxl', sheet_name=DFConstant.Brac_maturity_sheet_name.value.replace('Template', '').strip(), dtype=str)
                maturity_data = maturity_data.dropna(how='all', axis='rows').replace(np.nan, '', regex=True)
                maturity_data.columns = maturity_data.columns.str.strip()  # Remove leading and trailing spaces from column name
                api_endpoint_url_maturity = f'{webapi_base_path}/dfwebapi/process_maturity_report_data'
                maturity_data_json = maturity_data.to_json(orient='records')
                maturity_payload = {
                    "user_name": DFConstant.AdminUser.value,
                    "data": maturity_data_json,
                    "file_name": file_name,
                    "file_path": e_file_path,
                    "bank_name": bank_name,
                    "country": country_name
                }
                maturity_response = requests.post(api_endpoint_url_maturity, json=maturity_payload)
                maturity_att_status = maturity_response.json().get('att_status')
                print("Maturity Status:", maturity_att_status)

                if maturity_att_status == 'Success' and credit_att_status == 'Success':
                    if os.path.exists(e_file_path):
                        shutil.move(e_file_path, f'{processed_files_path}{file_name}')       
                else:
                    if os.path.exists(e_file_path):
                        shutil.move(e_file_path, f'{error_files_path}{file_name}')
                        print("Maturity Status:", maturity_att_status)
        else:
            print("No unread credit limit emails")

    except Exception as e:
        Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
        print('Error in process_bank_credit_data_City method: ', Except)   
        html_c = f""" <html><p>{Except}</p></html> """
        common.sendHtmlEmail(email_text, html_c, err_to_emails, '', session, '')
    finally: 
        endtime = datetime.now() - starttime
        print("Completed in ", endtime)
  
#process_credit_maturity_data_Brac_Bank()

with DAG(
		dag_id="Bank_Credit_Data_Processor_Brac_Bank",
		schedule_interval="*/30 * * * *",
		default_args={
			"owner": "airflow",
			"retries":2,
			"retry_delay":timedelta(minutes=10),
			"start_date": datetime(2023,12,29),
		},
		catchup=False) as dag1:
		
	process_credit_maturity_data_City_Bank=PythonOperator(
		task_id= "process_credit_maturity_data_Brac_Bank",
		python_callable=process_credit_maturity_data_Brac_Bank)
 
process_credit_maturity_data_Brac_Bank