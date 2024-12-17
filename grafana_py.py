import warnings
import numpy as np

warnings.filterwarnings("ignore", category=FutureWarning)
import pandas as pd
import uvicorn
from pathlib import Path
from fastapi import FastAPI, Request
import datetime
import json
from fastapi import APIRouter
from fastapi.middleware.cors import CORSMiddleware
router = APIRouter(tags=["默认路由"])

app = FastAPI()
app.add_middleware(
  CORSMiddleware,
  # 允许跨域的源列表，例如 ["http://www.example.org"] 等等，["*"] 表示允许任何源
  allow_origins=["*"],
  # 跨域请求是否支持 cookie，默认是 False，如果为 True，allow_origins 必须为具体的源，不可以是 ["*"]
  allow_credentials=False,
  # 允许跨域请求的 HTTP 方法列表，默认是 ["GET"]
  allow_methods=["*"],
  # 允许跨域请求的 HTTP 请求头列表，默认是 []，可以使用 ["*"] 表示允许所有的请求头
  # 当然 Accept、Accept-Language、Content-Language 以及 Content-Type 总之被允许的
  allow_headers=["*"],
)

class JsonResponse(object):
  def __init__(self, code, msg, data):
    self.code = code
    self.msg = msg
    self.data = data

  @classmethod
  def seccess(cls, code=200, msg='success', data=None):
    return cls(code, msg, data)

  @classmethod
  def fail(cls, code=400, msg='fail', data=None):
    return cls(code, msg, data)

  def to_dict(self):
    return json.dumps({
      "code": self.code,
      "msg": self.msg,
      "data": self.data
    })

@app.get("/")
async def root():
  return JsonResponse.seccess(msg='链接通过', data='0')


# 产品列表
@app.get("/table/ProductList")
async def ProductList():
    a = pd.read_csv('换手率.csv')
    a = a.to_json(force_ascii=False)
    return a

if __name__ == '__main__':
  uvicorn.run(f'{Path(__file__).stem}:app', host="101.132.178.232", port=5559)
  pass