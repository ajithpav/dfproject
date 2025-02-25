# FastAPI Web API

This project is a FastAPI-based web API providing various endpoints for processing payments, handling bank transactions, managing notifications, and generating reports.

## Table of Contents
- [Installation](#installation)
- [Running the Application](#running-the-application)
- [API Endpoints](#api-endpoints)
  - [Miscellaneous](#miscellaneous)
  - [ICICI Bank](#icici-bank)
  - [Axis Bank](#axis-bank)
  - [Notifications](#notifications)
  - [Maturity Reports](#maturity-reports)
  - [Standard Chartered Bank](#standard-chartered-bank)
  - [Common Bank Operations](#common-bank-operations)
- [Project Structure](#project-structure)
- [License](#license)

## Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/your-repo/fastapi-webapi.git
   cd fastapi-webapi
   ```

2. Create a virtual environment and activate it:
   ```sh
   python -m venv venv
   source venv/bin/activate  # On macOS/Linux
   venv\Scripts\activate     # On Windows
   ```

3. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

## Running the Application

Run the FastAPI server using:
```sh
uvicorn main:app --reload
```
This starts the server at `http://127.0.0.1:8000`.

Access the interactive API documentation at:
- Swagger UI: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- Redoc: [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

## API Endpoints

### Miscellaneous
- `POST /code_and_refresh_token_generator`
- `POST /process_payment_terms`
- `POST /reset_tax_dec_response`

### ICICI Bank
- `POST /process_bank_credit_ICICI`
- `POST /process_bank_discount_ICICI`

### Axis Bank
- `POST /process_bank_credit_Axis`
- `POST /process_bank_discount_Axis`

### Notifications
- `POST /user_notification_data_processor`
- `POST /credit_notification_processor`

### Maturity Reports
- `POST /process_maturity_report_data`
- `POST /maturity_report_notification_processor`
- `POST /send_discount_scheme_email`

### Standard Chartered Bank
- `POST /process_bank_credit_Standard_Chartered`
- `POST /process_bank_discount_Standard_Chartered`

### Common Bank Operations
- `POST /process_bank_credit_Common`

## Project Structure
```
fastapi-webapi/
├── controllers/
│   ├── api_controller.py  # Handles API logic
├── models/
│   ├── data_models.py  # Defines data models
├── routers/
│   ├── api_router.py  # Defines API routes
├── main.py  # Entry point for FastAPI app
├── requirements.txt  # Python dependencies
├── README.md  # Documentation
```

## License
This project is licensed under the MIT License.

