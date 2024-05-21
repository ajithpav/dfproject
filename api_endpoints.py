from fastapi import APIRouter
import sys
sys.path.append("..")

from controllers.api_controller import WebAPIController
from models.data_models import CodeToken, FileData, NotiAlertData, NotiData

router = APIRouter()
tag = "WebAPI"

#region misc
@router.post("/code_and_refresh_token_generator", tags=[tag])
async def code_and_refresh_token_generator(content: CodeToken):
    return await WebAPIController.code_and_refresh_token_generator(**content.dict())
    

@router.post("/process_payment_terms", tags=[tag])
async def process_payment_terms(file_data: FileData):
    return await WebAPIController.process_payment_terms(**file_data.dict())

@router.post("/reset_tax_dec_response", tags=[tag])
async def reset_tax_dec_response(file_data: FileData):
    return await WebAPIController.reset_tax_dec_response(**file_data.dict())
#endregion

#region ICICI
@router.post("/process_bank_credit_ICICI", tags=[tag])
async def process_bank_credit_ICICI(file_data: FileData):
    return await WebAPIController.process_bank_credit_ICICI(**file_data.dict())

@router.post("/process_bank_discount_ICICI", tags=[tag])
async def process_bank_discount_ICICI(file_data: FileData):
    return await WebAPIController.process_bank_discount_ICICI(**file_data.dict())
#endregion

#region Axis
@router.post("/process_bank_credit_Axis", tags=[tag])
async def process_bank_credit_Axis(file_data: FileData):
    return await WebAPIController.process_bank_credit_Axis(**file_data.dict())

@router.post("/process_bank_discount_Axis", tags=[tag])
async def process_bank_discount_Axis(file_data: FileData):
    return await WebAPIController.process_bank_discount_Axis(**file_data.dict())
#endregion

#region notifications
@router.post("/user_notification_data_processor", tags=[tag])
async def user_notification_data_processor(alert_data: NotiAlertData):
    return await WebAPIController.user_notification_data_processor(**alert_data.dict())

@router.post("/credit_notification_processor", tags=[tag])
async def credit_notification_processor():
    return await WebAPIController.credit_notification_processor()
#endregion

#region Maturity
@router.post("/process_maturity_report_data", tags=[tag])
async def process_maturity_report_data(file_data: FileData):
    return await WebAPIController.process_maturity_report_data(**file_data.dict())

@router.post("/maturity_report_notification_processor", tags=[tag])
async def maturity_report_notification_processor(bank_name: str):
    return await WebAPIController.maturity_report_notification_processor(bank_name)

@router.post("/send_discount_scheme_email", tags=[tag])
async def send_discount_scheme_email(noti_data: NotiData):
    return await WebAPIController.send_discount_scheme_email(**noti_data.dict())
#endregion

#region SCB
@router.post("/process_bank_credit_Standard_Chartered", tags=[tag])
async def process_bank_credit_Standard_Chartered(file_data: FileData):
    return await WebAPIController.process_bank_credit_Standard_Chartered(**file_data.dict())

@router.post("/process_bank_discount_Standard_Chartered", tags=[tag])
async def process_bank_discount_Standard_Chartered(file_data: FileData):
    return await WebAPIController.process_bank_discount_Standard_Chartered(**file_data.dict())

# @router.post("/process_maturity_report_Standard_Chartered", tags=[tag])
# async def process_maturity_report_Standard_Chartered(file_data: FileData):
#     return await WebAPIController.process_maturity_report_Standard_Chartered(**file_data.dict())
#endregion

#region brac & city
@router.post("/process_bank_credit_Common", tags=[tag])
async def process_bank_credit_Common(file_data: FileData):
    return await WebAPIController.process_bank_credit_Common(**file_data.dict())
#endregion