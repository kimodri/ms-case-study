import azure.functions as func
from src.extract import fetch_and_upload

def main(mytimer: func.TimerRequest):
    fetch_and_upload()
