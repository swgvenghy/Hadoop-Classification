import os
import re
import json
import time
import requests
import numpy as np 
import pandas as pd
from konlpy.tag import Okt


def regex(self, text):
    """
    거슬리는 데이터들 삭제
    정규 표현식 패턴 (인터넷 주소와 "© NAVER Corp" 삭제)
    한 단어 이하 삭제
    공백 제거
    시간 형식 삭제
    """
    pattern = r"(© NAVER Corp|https?://\S+|www\.\S+|m\.\S+|\S+\.\S+\.\S+)"
    cleaned_text = re.sub(pattern, "", text)
    cleaned_text = re.sub(r"\b\d{3,4}-\d{3,4}-\d{3,4}\b", "", cleaned_text)# 전화번호 형식 삭제 (예: 0507-1311, 010-1234-5678)
    cleaned_text = re.sub(r"\b\d+\b", "", cleaned_text)  # 숫자만 있는 부분을 제거
    cleaned_text = re.sub(r"\b\d{1,2}:\d{2}\b", "", cleaned_text) # 시간 형식 제외
    cleaned_text = re.sub(r"\b\w\b", "", cleaned_text) # 공백 제거
    cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip() # 한 단어 이하 삭제


