from xmlrpc.client import DateTime
from pydantic import BaseModel, Required
from typing import Optional
from datetime import datetime

class CodeToken(BaseModel):
    request_type: str
    code: str

class FileData(BaseModel):
    user_name : str
    data: str
    file_name: str
    file_path : str
    bank_name : str
    country : str

class NotiAlertData(BaseModel):
    user_name : str
    notice_name: str
    notice_subject: str
    all_sales_users: bool
    all_distributor_users: bool
    all_credit_managers: bool
    all_head_office_users: bool
    recp_data_list: list

class NotiData(BaseModel):
    user_name : str
    notification_id: str
    notification_description: str
    notification_type : str
    noti_content : str
    email_content : str
