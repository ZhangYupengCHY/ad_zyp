import hashlib
import json
import random
import string
import time
from json import JSONDecodeError

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd

class CompanyApiBase():
    @staticmethod
    def create_signature(token):
        timestamp = str(int(time.time()))
        nonce = ''.join(random.sample(string.ascii_letters + string.digits, 8))
        tmp_str = "".join(sorted([token, timestamp, nonce]))
        sha = hashlib.sha1(tmp_str.encode('utf-8'))
        signature = sha.hexdigest()
        return {
            'timestamp': timestamp,
            'nonce': nonce,
            'signature': signature
        }

    def get_php_token(self,url,token_user,token_psw):
        # 生成token
        token_dict = self.produce_php_token(url,token_user,token_psw)
        if isinstance(token_dict, dict):
            print(token_dict,'phptoken')
            php_token = token_dict["token"]

    @staticmethod
    def get_java_token(username='prod_ads', password='1XQP767x7x9bnRvnQ'):
        # url = "http://oauth.java.yibainetwork.com/oauth/" \
        #       "token?grant_type=client_credentials"

        java_token_url = "http://oauth.java.yibainetwork.com"
        url =java_token_url+"/oauth/token?grant_type=client_credentials"
        # token_request = requests.post(url, auth=HTTPBasicAuth(account, password))
        token_request = requests.post(url, auth=HTTPBasicAuth(username, password))
        token_result = token_request.content
        token_dict_content = json.loads(token_result.decode())
        # print(token_dict_content,'javatoken')
        now_token = token_dict_content['access_token']
        return now_token

    def produce_php_token(self,url,token_user,token_psw):
        javatoken=self.get_token()
        res = requests.post(url+f"?access_token={javatoken}",
            json={"username":token_user , "password": token_psw}
        )

        if res.status_code == 200:
            try:
                result_dict = res.json()
                # 如果是token超时则再申请一次token
                if "Access token expired" in result_dict.get("error_description", ""):
                    now_token = self.get_token()

                    new_token_url = url + f"?access_token={now_token}"
                    # 再次请求
                    res = requests.post(
                        new_token_url,
                        json={"username":token_user , "password": token_psw}
                    )
                    try:
                        result_dict = res.json()
                    except JSONDecodeError:
                        return res.content.decode()
                # 更新token
                return result_dict
            except JSONDecodeError:
                return res.content.decode()
        else:
            raise Exception(res.content.decode())

    def produce_token_info(self,url,token_user,token_psw):
        php_token = self.get_php_token(url,token_user,token_psw)
        signature_dict = self.create_signature(php_token)
        signature_dict["token"] = php_token
        signature_dict["username"] = token_user
        return signature_dict

    """
    def update_cab_status(self,sku_list_orgin,url_data,n0):
            url_token=have_send_num_to_record_url + "/mrp/api/getToken"
            token_user=token_user_php
            token_psw = token_psw_php
            # 处理基础数据
            m=int(len(sku_list_orgin)/n0)+1

            list_fail=[]
            for m0 in range(m):
                print(m0)
                sku_list=sku_list_orgin[m0*n0:m0*n0+n0]
                token_info = self.produce_token_info(url_token, token_user, token_psw)
                request_body = MultipartEncoder(
                    {
                        "data": json.dumps(sku_list),
                        "token_info": json.dumps(token_info)
                    }
                )
                request_header = {
                    "Content-Type": request_body.content_type
                }
                # 发起请求
                res = requests.post(
                    url_data,
                    data=request_body, headers=request_header
                )
                if res.status_code == 200:
                    try:
                        result_dict = res.json()
                        if result_dict['status']!=1:
                            list_fail=list_fail+sku_list
                        print(result_dict)
                    except :
                        result_dict={'status':'erro!!'}
                        print('erro!!')
                else:
                    result_dict={'status':'erro!!'}
                    print(result_dict)
                time.sleep(2)
            if len(list_fail)!=0:
                df_fail=pd.DataFrame(list_fail)
                config.tosql(df_fail,'fbm_fail')
            else:
                pass
    """


if __name__ == '__main__':
    print(PlanPhpApiBase().get_java_token(username='PY_DMuser_TEST',password='tios7z'))