# core/deps.py
from fastapi import Request
def get_app_state(request: Request):
    return request.app.state