import requests

def post(url, data=None, headers=None):
    try:
        return requests.post(url, json=data, headers=headers)
    except Exception as e:
        print(e)
        return str(e)

def get(url, headers=None):
    try:
        return requests.get(url, headers=headers)
    except Exception as e:
        print(e)
        return str(e)