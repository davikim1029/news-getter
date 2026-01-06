# core/deps.py
from fastapi import Request

def get_redis(request: Request):
    return request.app.state.redis

def get_app_state(request: Request):
    return request.app.state