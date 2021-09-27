# -*- coding: utf-8 -*-
"""
Proj: ad_helper
Created on:   2020/5/25 11:23
@Author: RAMSEY

Standard:
    s: data start
    t: important  temp data
    r: result
    error1: error type1 do not have file
    error2: error type2 file empty
    error3: error type3 do not have needed data
"""
import shutil
import zipfile

import pandas as pd
import os
import string
import redis
import re
import MeCab
import jieba
import time
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, wordpunct_tokenize
import langid
import pymysql
from datetime import datetime, timedelta
import pickle
import requests
import rsa
import base64
import requests
import json

from retry import retry
from sqlalchemy import create_engine

import my_toolkit.process_files as process_files


# 文件的缩写
FILE_ABBR = ('cp', 'ac', 'ol', 'st', 'ao', 'br')

# 文件识别标识
# 广告报表中的拥有:产品报表cp和品牌报表bd
FILE_RECOGNIZE = {'cp': 'bulk', 'ac': 'active', 'st': 'search', 'ao': 'order', 'br': "business",'bd':'bulk'}

# 站点名
SITES = ('CA', 'DE', 'FR', 'IT', 'SP', 'JP', 'UK', 'MX', 'IN', 'US', 'ES', 'AU', 'AE')

# 站点的出价常量
# 包括了
# 1.站点实际汇率
# 2.实际给的bid的转换率
# 3.广告组最小出价
# 4.理想的acos
# 5.cpc最高出价
# 6.站点的域名
SITES_PARAM = {
    'exchange_rate':
        {'CA': 0.7519, 'DE': 1.0981, 'FR': 1.0981, 'IT': 1.0981, 'SP': 1.0981, 'JP': 0.009302,
         'UK': 1.2445, 'MX': 0.05147, 'IN': 0.01412, 'US': 1, 'ES': 1.0981, 'AU': 0.6766, 'AE': 0.2723},
    'bid_exchange':
        {'CA': 1, 'DE': 1, 'FR': 1, 'IT': 1, 'SP': 1, 'JP': 0.009302,
         'UK': 1, 'MX': 0.05147, 'IN': 0.01412, 'US': 1, 'ES': 1, 'AU': 0.6766},
    'ad_group_min_bid':
        {'CA': 0.02, 'DE': 0.02, 'FR': 0.02, 'IT': 0.02, 'SP': 0.02, 'JP': 2,
         'UK': 0.02, 'MX': 0.1, 'IN': 0.1, 'US': 0.02, 'ES': 0.02, 'AU': 0.1, 'AE': 0.24},
    'acos_ideal':
        {'CA': 0.14, 'DE': 0.15, 'FR': 0.15, 'IT': 0.15, 'SP': 0.15, 'JP': 0.15,
         'UK': 0.18, 'MX': 0.15, 'IN': 0.18, 'US': 0.18, 'ES': 0.15, 'AU': 0.15},
    'cpc_max':
        {'CA': 0.4, 'DE': 0.35, 'FR': 0.35, 'IT': 0.3, 'SP': 0.3, 'JP': 25,
         'UK': 0.4, 'MX': 2.5, 'IN': 4.5, 'US': 0.5, 'ES': 0.3, 'AU': 0.4},
    'site_web':
        {'US': 'Amazon.com', 'CA': 'Amazon.ca', 'FR': 'Amazon.fr', 'UK': 'Amazon.co.uk', 'DE': 'Amazon.de',
         'ES': 'Amazon.es', 'IT': 'Amazon.it', 'JP': 'Amazon.jp', 'MX': 'Amazon.com.mx', 'IN': 'Amazon.in',
         'AU': 'Amazon.com.au'},
    'campaign_budget':
        {'CA': 200, 'DE': 200, 'FR': 200, 'IT': 200, 'SP': 200, 'JP': 20000,
         'UK': 200, 'MX': 3800, 'IN': 14000, 'US': 200, 'ES': 200, 'AU': 200}
}

# 广告组服务器连接引擎
ENGINE_AD_SERVER = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
        'marmot', 'marmot123', '127.0.0.1', 3306, 'team_station', 'utf8'))



# 站点名中英文转换
COUNTRY_CN_EN_DICT = {'日本': 'JP', '印度': 'IN', '澳大利亚': 'AU', '美国': 'US', '加拿大': 'CA', '墨西哥': 'MX',
                      '英国': 'UK', '法国': 'FR', '德国': 'DE', '西班牙': 'ES', '意大利': 'IT', '中东': 'AE', '中国': 'ZH',
                      '荷兰': 'NL','巴西': 'BR', '瑞典': 'SE', '新加坡': 'SG', '沙特': "SA",'波兰':'PL','土耳其':'TR'}


# 计算脚本运行时间
def run_time(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        cost_time = end_time - start_time
        print(f'{func.__name__}花费:{cost_time}')
        return result

    return wrapper


# 检测df的有效性
def detect_df(df):
    """
    检查df的有效性
        数据是否存在
        数据类型是否是pd.DataFrame
        数据是否为空
    :param df: 原始的df
    :return: None
    """
    if df is None:
        return False
    if not isinstance(df, pd.DataFrame):
        return False
    return True


# 处理redis
class Redis_Store(redis.StrictRedis):
    """
    继承redis.StrictRedis类,使用redis来进行操作:
        1.存入五表的数据
        2.存入list数据
        3.存入dataframe
        4.存入string
    """

    def __init__(self, host='127.0.0.1', port=6379, db=2,
                 password='', decode_responses=True, expire_time=None):
        redis.StrictRedis.__init__(self, host=host, port=port, password=password, db=db,
                                   decode_responses=decode_responses)
        self.redis_pool = redis.ConnectionPool(host=host, port=port, password=password,
                                               db=db, decode_responses=decode_responses)
        self.red = redis.StrictRedis(connection_pool=self.redis_pool)
        self.expire_time = expire_time
        self.db=db

    # 获得包含某个字符串的全部键值
    def include(self,sign:str or list):
        all_keys = self.red.keys()
        if isinstance(sign,str):
            keys_contains = [key for key in all_keys if sign in key]
        if isinstance(sign,list):
            keys_contains = []
            for key in all_keys:
                for one_sign in sign:
                    if one_sign not in key:
                        break
                else:
                    keys_contains.append(key)
        return {key:self.red.get(key) for key in keys_contains}


    # 将dataframe存储到redis中
    def redis_upload_df(self, key, data):
        """
        将DataFrame存储到redis中
        Args:
            key:str
                保存DataFrame的键
            data:DataFrame
                 DataFrame数据

        Returns:None

        """
        df_bytes = data.to_msgpack(encoding='utf-8')
        self.red.set(key, df_bytes)

    # 将redis中数据以DataFrame取出来
    def redis_download_df(self, key):
        """

        Args:
            key: str
                存储binary型数据的redis键

        Returns:DataFrame

        """
        df_from_redis = pd.read_msgpack(self.red.get(key))
        return df_from_redis


    def refresh_df(self,df,keyName,addDateTime=True):
        """更新redis中的df"""
        if addDateTime is True:
            dateNowStr = datetime.strftime(datetime.now(),'%Y-%m-%d_%H-%M-%S')
        # 先删除已经有的
        existKey = [key for key in self.keys() if keyName in key]
        if existKey:
            self.red.delete(*existKey)
        self.redis_upload_df(f'{keyName}$${dateNowStr}',df)


    def download_df(self, key,keyWord=False):
        """

        Args:
            key: str
                存储binary型数据的redis键

        Returns:DataFrame

        """
        redTemp = Redis_Store(db=self.db,decode_responses=False)
        if keyWord is False:
            if key in self.red.keys():
                return pd.read_msgpack(redTemp.get(key))
            else:
                return
        else:
            chooseKey = [key_ for key_ in self.red.keys() if key in key_]
            if chooseKey:
                chooseKey = chooseKey[0]
                return pd.read_msgpack(redTemp.get(chooseKey))
            else:
                return
# 货币转换
def currency_trans(currency) -> 'digit':
    """
    将货币装换成数字
    逻辑:
        通过判断倒数第三位是否是,(逗号)或是.(点号)来判断是小数还是整数
    :param currency:需要转换的货币
    :return: 整型或浮点型货币
    """
    if pd.isnull(currency):
        return
    if not isinstance(currency, str):
        return
    else:
        currency = currency.strip(' ')
        currency_temp = re.findall('\d.*', currency)
        if len(currency_temp) == 1:
            currency_temp = currency_temp[-1]
            try:
                if currency_temp[-3] in [',', '.']:
                    # 该数字为包含两位小数的数字
                    return float(re.sub('[,.]', '', currency_temp)) / 100
                else:
                    # 该数字不包含两位小数的数字
                    return int(re.sub('[,.]', '', currency_temp))
            except:
                return int(re.sub('[,.]', '', currency_temp))
        if not currency_temp:
            return
        if len(currency_temp) > 1:
            return


# 获得redis中时间范围内的某几种类型的报表
def get_station_updatetime(need_file_types='all', date_before=0):
    """
    获得redis中最新的某几种类型的报表
    Parameters:
        need_file_types:list or set or 'all'
                     sublist of ['CP','BR','AC','AO','ST'] or 'all' default
        date_before:int default 0
                    距离今日时间间隔
    :return:list
            一段日期内拥有需求的站点名:更新时间集合
    """
    # 1.判断需要列表的正确性
    all_files_kws = set(['CP', 'BR', 'AC', 'AO', 'ST', ])
    if need_file_types == 'all':
        need_file_types = all_files_kws
    else:
        if not isinstance(need_file_types, (list, set)):
            raise ValueError(f'{need_file_types} is not list or set or "all"')
        need_files = [file_type.upper() for file_type in need_file_types]
        if not set(need_files).issubset(all_files_kws):
            raise ValueError(f"{need_files} is not subset of {'CP', 'BR', 'AC', 'AO', 'ST'}.")

    # 2.判断redis中哪些站点拥有今日广告报表和商业报表
    connect_redis = Redis_Store()
    redis_keys = connect_redis.keys()
    five_files_keys_sign = 'FIVE_FILES_KEYS_SAVE'
    five_files_keys = [key for key in redis_keys if five_files_keys_sign in key]
    start_date = datetime.now().date()
    start_date -= timedelta(days=date_before)
    start_date = int(start_date.strftime('%Y%m%d'))
    today_keys = set([key for key in five_files_keys if (key[-14:-6].isdigit()) and (int(key[-14:-6]) >= start_date)])
    today_stations = {key[len(five_files_keys_sign) + 1:-15]: key[-14:] for key in today_keys}
    completed_station_dict = dict()
    for station, updatetime in today_stations.items():
        station_updatetime = updatetime
        file_count = 0
        station = station[:-3]
        for file_key in today_keys:
            file_type = file_key[-17:-15]
            if (station in file_key) and (file_type in need_file_types):
                file_count += 1
                if file_count == len(need_file_types):
                    break
        if file_count == len(need_file_types):
            station_info = {station: station_updatetime}
            completed_station_dict.update(station_info)
    connect_redis.close()
    return completed_station_dict


# 获得某个站点某个表数据
def get_station_data(station_name, type_kw):
    """
    获得五表中某个表的数据
    Parameters:
        station_name: str
                    站点名
        type_kw: str
                one of ['CP','BR','AC','AO','ST']
    Returns: DataFrame
    """
    connect_redis = Redis_Store()
    redis_all_keys = connect_redis.keys()
    redis_file_keys_sign = 'FIVE_FILES_KEYS_SAVE'
    station_key = [key for key in redis_all_keys if
                   (redis_file_keys_sign in key) and (station_name.upper() in key) and (type_kw.upper() in key)]
    if not station_key:
        return None
    else:
        station_key = station_key[0]
        file_path = connect_redis.get(station_key)
        file_data = process_files.read_pickle_2_df(file_path)
        return file_data


# 获得文件夹中更新的文件
def folder_update_file(folder, file_sign_word=None, refresh=1):
    """
    获得文件夹中最新的更新文件,每一秒刷新目录，去获得最新的文件夹
    :param folder: path
            文件夹名
    :param file_sign_word:None or str default None
            文件的标识
    :param refresh:int default 1
            目录刷新的时间
    :return:list or None
            文件夹下更新的文件列表,若没有文件更新，则返回空
    """
    if not os.path.exists(folder):
        return
    if file_sign_word is None:
        file_sign_word = '.'
    files_list = [os.path.join(folder, file) for file in os.listdir(folder) if file_sign_word in file]
    if not files_list:
        return
    files_modify_time = {file: os.path.getmtime(file) for file in files_list}
    if refresh <= 0:
        refresh == 0.1
    time.sleep(refresh)
    new_files_list = [os.path.join(folder, file) for file in os.listdir(folder) if file_sign_word in file]
    files_modify_time_new = {file: os.path.getmtime(file) for file in new_files_list}
    new_file = [file for file, file_time in files_modify_time_new.items() if
                file_time != files_modify_time.get(file, None)]
    return new_file


# 文字翻译
def translate_words(words, langs='en'):
    """
    将文字按照要求翻译
    Args:
        words: list,str
            需要翻译的文字
        langs: list,str default 'en'
            将文字翻译成的语言

    Returns:dict
        文字翻译的结果
        {word_lang:trans_word}
        {'跑_zh-CN':'run'}

    """
    region_lang_dict = {'ar': '阿拉伯语', 'ko': '朝鲜语', 'de': '德语', 'ru': '俄语', 'fr': '法语',
                        'zh': '汉语', 'la': '拉丁语', 'pt': '葡萄牙语', 'ja': '日语', 'es': '西班牙语',
                        'en': '英语', 'it': '意大利语', 'hi': '印地语', 'th': '泰国语',
                        'ms': '马来西亚语', 'id': '印度尼西亚语', 'lo': '老挝语', 'fil': '菲律宾语'}
    request_url = 'http://rest.java.yibainetwork.com/util/translate/comboTranslate/'
    access_token = '?access_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6WyJyZWFkIl0sImV' \
                   '4cCI6MjU2ODc5NDY2MSwiYXV0aG9yaXRpZXMiOlsiOCJdLCJqdGkiOiIzMzY0ZTg2Ni1jODI2LTQyZjMt' \
                   'OTc1YS01NzJiNzVjNTdmNmYiLCJjbGllbnRfaWQiOiJjbGllbnQifQ.5TAlVr1Ew4tBDIpcTVwrwyOZej0' \
                   'xVrS8xHfAFRmiCkg'
    upload_url = request_url + access_token
    if isinstance(words, str) and isinstance(langs, str):
        upload_param = {
            'key': {
                "text": words,
                "language": langs,
            }
        }
        response = requests.post(url=upload_url, json=upload_param)

        return {words: json.loads(response.content.decode(encoding='utf-8'))['key']['data']}

    if isinstance(words, list):
        if isinstance(langs, str):
            langs = [langs] * len(words)

        if isinstance(langs, list):
            if len(words) != len(langs):
                raise ValueError('langs choose number must equal words numbers.')
        count = 0
        upload_param = {}
        for word, lang in zip(words, langs):
            if word is None:
                continue
            key = word + '_' + lang
            upload_param_temp = {
                key: {
                    "text": word,
                    "language": lang,
                }
            }
            upload_param.update(upload_param_temp)
            count += 1
        response = requests.post(url=upload_url, json=upload_param)
        response_text = json.loads(response.content.decode(encoding='utf-8'))
        print({key: value['data'] for key, value in response_text.items()})

        translated_words = []

        for word, lang in zip(words, langs):
            if word is None:
                temp_word = ''
                translated_words.append(temp_word)
                continue
            for key in response_text.keys():
                if word + '_' + lang == key:
                    temp_word = response_text[key]['data']
                    translated_words.append(temp_word)
                    break

        return translated_words


# 将文字拆分
def split_sentence(sentence: str, lang='english', splitbypunct=True):
    """
    将一句话分解成一个List,List中剔除停用词
    日语使用 maceb库: 安装教程 https://blog.csdn.net/ZYXpaidaxing/article/details/81913708
    中文使用 jieba库:
    其他语言使用 tokenize库:
    #todo 自动识别输入的语言,然后进行分词.
    Args:
        sentence: str
            句子
        lang: str default english
            停用词语言
        splitbypunct:bool default True
            是否是按照标点分隔
    Returns:list
            句子分解成的list
    """
    if not isinstance(sentence, str):
        raise ValueError('Import sentence must  str type.')

    # 支持的语言
    support_langs = set(stopwords._fileids)

    # 添加自定义的中文与日文
    self_defind_lang = set(['chinese', 'japanese'])

    support_langs.update(self_defind_lang)

    # todo 语言缩写字典
    lang = lang.lower()
    if lang.lower() not in support_langs:
        print('Lang you input should one of langs as follows.')
        print('------------------------------------------------')
        print(f'{support_langs}')
        raise ValueError('Please fill lang again. ')

    # 输入为日语
    if lang == 'japanese':
        # 日语停用词
        jp_stop_words = set(["あっ", "あり", "ある", "い", "いう", "いる", "う",
                             "うち", "お", "および", "おり", "か", "かつて", "から",
                             "が", "き", "ここ", "こと", "この", "これ", "これら", "さ",
                             "さらに", "し", "しかし", "する", "ず", "せ", "せる", "そして",
                             "その", "その他", "その後", "それ", "それぞれ", "た", "ただし",
                             "たち", "ため", "たり", "だ", "だっ", "つ", "て", "で", "でき", "できる",
                             "です", "では", "でも", "と", "という", "といった", "とき", "ところ", "として",
                             "とともに", "とも", "と共に", "な", "ない", "なお", "なかっ", "ながら", "なく",
                             "なっ", "など", "なら", "なり", "なる", "に", "において", "における", "について",
                             "にて", "によって", "により", "による", "に対して", "に対する", "に関する", "の",
                             "ので", "のみ", "は", "ば", "へ", "ほか", "ほとんど", "ほど", "ます", "また", "または",
                             "まで", "も", "もの", "ものの", "や", "よう", "より", "ら", "られ", "られる", "れ", "れる",
                             "を", "ん", "及び", "特に"])
        mecab = MeCab.Tagger("-Owakati")
        sent_list = mecab.parse(sentence)
        return [word for word in sent_list.split(' ') if word not in jp_stop_words and word not in string.printable]

    # 输入为中文
    if lang == 'chinese':
        # 中文停用词
        zh_stop_words = set(
            ["、", "。", "〈", "〉", "《", "》", "一", "一切", "一则", "一方面", "一旦", "一来", "一样", "一般", "七", "万一", "三", "上下", "不仅",
             "不但",
             "不光", "不单", "不只", "不如", "不怕", "不惟", "不成", "不拘", "不比", "不然", "不特", "不独", "不管", "不论", "不过", "不问", "与", "与其",
             "与否", "与此同时", "且", "两者", "个", "临", "为", "为了", "为什么", "为何", "为着", "乃", "乃至", "么", "之", "之一", "之所以", "之类",
             "乌乎",
             "乎", "乘", "九", "也", "也好", "也罢", "了", "二", "于", "于是", "于是乎", "云云", "五", "人家", "什么", "什么样", "从", "从而", "他",
             "他人",
             "他们", "以", "以便", "以免", "以及", "以至", "以至于", "以致", "们", "任", "任何", "任凭", "似的", "但", "但是", "何", "何况", "何处",
             "何时",
             "作为", "你", "你们", "使得", "例如", "依", "依照", "俺", "俺们", "倘", "倘使", "倘或", "倘然", "倘若", "借", "假使", "假如", "假若", "像",
             "八", "六", "兮", "关于", "其", "其一", "其中", "其二", "其他", "其余", "其它", "其次", "具体地说", "具体说来", "再者", "再说", "冒", "冲",
             "况且",
             "几", "几时", "凭", "凭借", "则", "别", "别的", "别说", "到", "前后", "前者", "加之", "即", "即令", "即使", "即便", "即或", "即若", "又",
             "及",
             "及其", "及至", "反之", "反过来", "反过来说", "另", "另一方面", "另外", "只是", "只有", "只要", "只限", "叫", "叮咚", "可", "可以", "可是",
             "可见",
             "各", "各个", "各位", "各种", "各自", "同", "同时", "向", "向着", "吓", "吗", "否则", "吧", "吧哒", "吱", "呀", "呃", "呕", "呗", "呜",
             "呜呼", "呢", "呵", "呸", "呼哧", "咋", "和", "咚", "咦", "咱", "咱们", "咳", "哇", "哈", "哈哈", "哉", "哎", "哎呀", "哎哟", "哗",
             "哟",
             "哦", "哩", "哪", "哪个", "哪些", "哪儿", "哪天", "哪年", "哪怕", "哪样", "哪边", "哪里", "哼", "哼唷", "唉", "啊", "啐", "啥", "啦",
             "啪达",
             "喂", "喏", "喔唷", "嗡嗡", "嗬", "嗯", "嗳", "嘎", "嘎登", "嘘", "嘛", "嘻", "嘿", "四", "因", "因为", "因此", "因而", "固然", "在",
             "在下", "地", "多", "多少", "她", "她们", "如", "如上所述", "如何", "如其", "如果", "如此", "如若", "宁", "宁可", "宁愿", "宁肯", "它",
             "它们",
             "对", "对于", "将", "尔后", "尚且", "就", "就是", "就是说", "尽", "尽管", "岂但", "己", "并", "并且", "开外", "开始", "归", "当", "当着",
             "彼",
             "彼此", "往", "待", "得", "怎", "怎么", "怎么办", "怎么样", "怎样", "总之", "总的来看", "总的来说", "总的说来", "总而言之", "恰恰相反", "您",
             "慢说",
             "我", "我们", "或", "或是", "或者", "所", "所以", "打", "把", "抑或", "拿", "按", "按照", "换句话说", "换言之", "据", "接着", "故", "故此",
             "旁人", "无宁", "无论", "既", "既是", "既然", "时候", "是", "是的", "替", "有", "有些", "有关", "有的", "望", "朝", "朝着", "本", "本着",
             "来",
             "来着", "极了", "果然", "果真", "某", "某个", "某些", "根据", "正如", "此", "此外", "此间", "毋宁", "每", "每当", "比", "比如", "比方",
             "沿",
             "沿着", "漫说", "焉", "然则", "然后", "然而", "照", "照着", "甚么", "甚而", "甚至", "用", "由", "由于", "由此可见", "的", "的话", "相对而言",
             "省得", "着", "着呢", "矣", "离", "第", "等", "等等", "管", "紧接着", "纵", "纵令", "纵使", "纵然", "经", "经过", "结果", "给", "继而",
             "综上所述", "罢了", "者", "而", "而且", "而况", "而外", "而已", "而是", "而言", "能", "腾", "自", "自个儿", "自从", "自各儿", "自家", "自己",
             "自身", "至", "至于", "若", "若是", "若非", "莫若", "虽", "虽则", "虽然", "虽说", "被", "要", "要不", "要不是", "要不然", "要么", "要是",
             "让",
             "论", "设使", "设若", "该", "诸位", "谁", "谁知", "赶", "起", "起见", "趁", "趁着", "越是", "跟", "较", "较之", "边", "过", "还是",
             "还有",
             "这", "这个", "这么", "这么些", "这么样", "这么点儿", "这些", "这会儿", "这儿", "这就是说", "这时", "这样", "这边", "这里", "进而", "连", "连同",
             "通过", "遵照", "那", "那个", "那么", "那么些", "那么样", "那些", "那会儿", "那儿", "那时", "那样", "那边", "那里", "鄙人", "鉴于", "阿", "除",
             "除了", "除此之外", "除非", "随", "随着", "零", "非但", "非徒", "靠", "顺", "顺着", "首先", "︿", "！", "＃", "＄", "％", "＆", "（",
             "）",
             "＊", "＋", "，", "０", "１", "２", "３", "４", "５", "６", "７", "８", "９", "：", "；", "＜", "＞", "？", "＠", "［", "］",
             "｛",
             "｜", "｝", "～", "￥"])
        # .cut_for_search 为搜索引擎模式
        # .cut(cut_all=True) 为全模式
        # .cut(cut_all=False) 为模糊模式
        sent_list = jieba.cut_for_search(sentence)  # 搜索引擎模式
        return [word for word in sent_list if word not in zh_stop_words and word not in string.printable]

    # 外文停用词
    if lang == 'english':
        stop_words = ["mightn't", 'hers', 'all', "you've", 'by', 'did', "won't", 'before', 'and', "didn't", 'up',
                      'such', 'hasn', 'after', 'below', 's', 'our', 'over', 'too', 'wasn', 'weren', "isn't", "wouldn't",
                      'me', 'itself', 'or', 'of', 'about', 'be', 'shan', 'off', 'you', 'between', "couldn't", 'under',
                      'yourselves', 'an', 'on', 'both', 'now', 'who', 'very', 'i', 'doing', "you're", 'have', 'once',
                      'other', "aren't", 'o', 'own', 'ourselves', "mustn't", 'how', 'was', 'had', 'couldn', 'haven',
                      "that'll", 'in', 'the', 'my', 'each', "doesn't", 're', 'am', 'if', 'your', 'his', 'been', 'out',
                      'just', "haven't", 'down', 'm', 'than', 'shouldn', 'why', 'through', 'its', 'nor', 't', "needn't",
                      "should've", 'd', 'ain', 'won', 'a', 'few', 'against', 'should', 'don', 'y', "wasn't", 'is',
                      "it's", 'here', 'their', 'herself', "shouldn't", 'mightn', 'no', 'from', 'them', 'didn',
                      'further', "hadn't", 'there', 'has', 'which', 'this', 'doesn', 'not', 'her', 'as', 'again',
                      'himself', 'most', 'will', 'myself', 'they', 'do', 'hadn', 'so', 'yours', 'to', 've', "you'd",
                      'isn', 'aren', 'needn', 'themselves', 'then', 'during', 'where', 'whom', 'when', 'being', 'what',
                      'he', "you'll", 'can', 'll', 'does', 'while', 'having', 'these', 'that', 'into', "she's", 'more',
                      'but', 'at', 'for', "hasn't", 'him', "don't", 'mustn', 'any', "weren't", "shan't", 'it', 'some',
                      'yourself', 'ours', 'we', 'those', 'were', 'above', 'are', 'she', 'because', 'theirs', 'until',
                      'same', 'only', 'with', 'ma', 'wouldn']
    else:
        stop_words = set(stopwords.words(lang))

    # 分词
    if splitbypunct:
        words_tokens = wordpunct_tokenize(sentence)
    if not splitbypunct:
        words_tokens = word_tokenize(sentence)

    return [word for word in words_tokens if word not in stop_words and word not in string.printable]


def detect_list_lang(words: list):
    """
    判断文字是什么语言:
        若语言的开头和结尾是以英文或是数字开头,这可以判断为是英语
        若语言的开头和结尾不是以英文或是数字开头,则需要判断文字是 最接近如下
            ['it', 'en', 'de', 'fr', 'es', 'ja', 'zh']
    Args:
        words:list
            需要检测的一组文字
    Returns:list
            文字的检测结果
    """
    list_lang = [detect_lang(word) for word in words]
    lang_dict = {'en': 'english', 'ja': 'japanese', 'zh': 'chinese', 'it': 'italian', 'de': 'german',
                 'fr': 'french', 'es': 'spanish'}
    return [lang_dict[lang] for lang in list_lang]


# 检测文字最有可能的语言
def detect_lang(words):
    langid.set_languages(['it', 'en', 'de', 'fr', 'es', 'ja', 'zh'])
    if not isinstance(words,str):
        words = str(words)
    array = langid.classify(words)
    lang = array[0]
    return lang


def init_df(df,fillna_value='',change_columns_type=None):
    """
    初始化df数据
    step1:
        删除df中的空格,填充空白值
    step2:
        重命名
    step3:
        修改数据类型
    Parameters
    ----------
    fillna_value : string,default ''
         缺失值充填
    change_columns_type :dict,None,default None
        keys must int,float,datetime,str
         --int:list
            需要转换成整形的字段
         --float:list
            需要转换成浮点型的字段
        --datetime:list
            需要转换成时间格式的字段
         --str:list
            需要转换成字符串类型的字段
    df :pd.DataFrame

    Returns
    -------

    """


    # 1. 去掉数据中和列名的空格
    # 1.1 去掉列名的前后空格
    df.columns = [column.strip() for column in df.columns]
    # 1.2 去掉df中的数据中的
    df = df.apply(lambda x: x.astype(str).str.strip() if x.dtype == "object" else x)
    # df = df.applymap(lambda x:x.strip() if isinstance(x,str) else x)

    # 2.填充空白值
    df.fillna(value=fillna_value,inplace=True,downcast='infer')

    # 3.修改数据类型
    if change_columns_type is not None:
        if not isinstance(change_columns_type,dict):
            raise TypeError('change_columns_type type must dict')
        else:
            init_types = ['int','datetime','float','str','percent']
            _change_columns_keys = change_columns_type.keys()
            if not set(_change_columns_keys).issubset(set(init_types)):
                unexpected_keys = set(_change_columns_keys) - set(init_types)
                raise TypeError(f'change type has unexpected key:{unexpected_keys}')
            else:
                # 转换为整形
                if 'int' in _change_columns_keys:
                    int_columns = change_columns_type['int']
                    df_columns_to_numeric(df, int_columns)
                    for col in int_columns:
                        if df[col].dtype not in ('int64','int32','int16','int8'):
                            df[col] = df[col].apply(int)

                # 转换为浮点型
                if 'float' in _change_columns_keys:
                    float_columns = change_columns_type['float']
                    df_columns_to_numeric(df, float_columns)
                # 转换时间序列
                if 'datetime' in _change_columns_keys:
                    datetime_columns = change_columns_type['datetime']
                    for column in datetime_columns:
                        df[column] = pd.to_datetime(df[column],yearfirst=True,dayfirst=False)
                if 'str' in _change_columns_keys:
                    str_columns = change_columns_type['str']
                    for column in str_columns:
                        df[column] = df[column].astype('str',errors='ignore')
                if 'percent' in _change_columns_keys:
                    percent_columns = change_columns_type['percent']
                    for column in percent_columns:
                        df[column] = series_numeric_to_percent(df[column])

    return df


# 列
class Queue:
    def __init__(self):
        self.items = []

    def enqueue(self, item):
        self.items.append(item)

    def enqueue_items(self,items):
        for item in items:
            self.items.append(item)

    def dequeue(self):
        return self.items.pop(0)


    def dequeue_items(self,num):
        items_value = []
        for _ in range(num):
            value = self.items.pop(0)
            items_value.append(value)
        return items_value


    def empty(self):
        return self.size() == 0

    def size(self):
        return len(self.items)



def percent_series_to_numeric(series,fill_na=0):
    """
    将带百分号的series转换为数值型
    :param series:
    :param fill_na:
    :return:
    """
    if not isinstance(series,pd.Series):
        return series
    else:
        series = series.str.strip('%')
        return pd.to_numeric(series,errors='coerce').fillna(value=fill_na,downcast='infer')/100


def series_numeric_to_percent(series,points_keep = 2):
    """
    将pd.Series转化为百分
    :param series:
    :return:
    """
    if not isinstance(series,pd.Series):
        return series
    else:
        if points_keep>0:
            return [str(round(value*100,points_keep))+"%" for value in series]
        else:
            return [str(int(value*100))+"%" for value in series]


def df_columns_to_numeric(df,columns,percent_columns=None,fill_na=0,inplace=True):
    """
     将df指定列转换为数值型
    Parameters
    ----------
    numeric_type : 'integer', 'float',None,default None
    df : pd.DataFrame
    columns :
    fill_na :
    inplace :

    Returns
    -------

    """
    if isinstance(df,pd.DataFrame) and isinstance(columns,list):
        if inplace is True:
            for column in columns:
                df[column] = series_to_numeric(df[column],fill_na=fill_na)
    if percent_columns is not None:
        for column in percent_columns:
            df[column] = percent_series_to_numeric(df[column],fill_na=fill_na)


# 将列转换为数值:整型或是浮点型(保留几位有效数据)
def trans_into_numerical(str_value, type='int', point=2, fill_na=0):
    """
    将字符串型数据转换为整型或是浮点型
    Args:


        str_value:str
            字符串转换为数值型
        type :str  int or float default int
            需要转换成的数据类型:整型或是浮点型
        point:int default 2
                若为浮点型,需要保留的浮点型小数点位数
        fill_na : numerical default 0
            空白的值用0填充
    Returns:int
        转换后的数值

    """
    if not isinstance(str_value, str):
        return str_value
    if type not in ['int', 'float']:
        raise ValueError('trans into numerical TYPE should be INT or FLOAT')
    if not isinstance(point, int):
        raise ValueError('trans into numerical POINT should be INT')
    str_value = str_value.strip()
    if pd.isna(str_value):
        return fill_na
    if str_value == '':
        return fill_na
    if type == 'int':
        return int(str_value)
    else:
        if str_value[-3] in [',', '.']:
            return round(float(re.sub('[^0-9]', '', str_value)) / 100, point)
        else:
            return round(float(re.sub('[^0-9]', '', str_value)), point)

def series_to_numeric(series,fill_na=0):
    """
    将series转换为数值型
    Parameters
    ----------
    series :
    fillna :

    Returns
    -------

    """
    if not isinstance(series,pd.Series):
        return series
    else:
        return pd.to_numeric(series,errors='coerce').fillna(value=fill_na,downcast='infer')


# 将列转换为数值:整型或是浮点型(保留几位有效数据)
def trans_into_numerical(str_value, type='int', point=2, fill_na=0):
    """
    将字符串型数据转换为整型或是浮点型
    Args:


        str_value:str
            字符串转换为数值型
        type :str  int or float default int
            需要转换成的数据类型:整型或是浮点型
        point:int default 2
                若为浮点型,需要保留的浮点型小数点位数
        fill_na : numerical default 0
            空白的值用0填充
    Returns:int
        转换后的数值

    """
    if not isinstance(str_value, str):
        return str_value
    if type not in ['int', 'float']:
        raise ValueError('trans into numerical TYPE should be INT or FLOAT')
    if not isinstance(point, int):
        raise ValueError('trans into numerical POINT should be INT')
    str_value = str_value.strip()
    if pd.isna(str_value):
        return fill_na
    if str_value == '':
        return fill_na
    if type == 'int':
        return int(str_value)
    else:
        if (len(str_value) > 3) and (str_value[-3] in [',', '.']):
            return round(float(re.sub('[^0-9]', '', str_value)) / 100, point)
        else:
            return round(float(re.sub('[^0-9]', '', str_value)), point)


def is_number(str_num):
    """
    判断字符串是否由数字或是小数点组成
    :param str_num:
    :return:
    """
    pattern = re.compile(r'^[-+]?\d*\.?\d*$')
    result = pattern.match(str_num)
    if result:
        return True
    else:
        return False


# 检测df的有效性
def is_valid_df(df):
    """
    检查df的有效性
        数据是否存在
        数据类型是否是pd.DataFrame
        数据是否为空
    :param df: 原始的df
    :return: None
    """
    if df is None:
        return False
    if not isinstance(df, pd.DataFrame):
        return False
    if df.empty:
        return False
    return True



def trans_erpsku_info_from_sql_2_redis():
    """
    从公司的服务器中加载erpsku信息,
    进行处理得到erpsku,asin,sellersku后,
    将信息存储到redis中其中
    key 以 erpsku_info_时间(日期_小时) db=0
    :return:None
    """
    # 加载erpsku信息
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
        'mrp_read', 'mrpread', '47.106.127.183', 3306, 'mrp_py', 'utf8'))
    conn = engine.connect()
    select_erpsku_sql = 'SELECT 标识,erp_sku FROM gross_require'
    erpsku_info = pd.read_sql(select_erpsku_sql, conn)
    conn.close()
    # 将数据库信息处理后生成erpsku,asin,sellersku
    """
    标识：
    'Socialme美国$SMU - JHX - 8
    WMb0P - JY18828 - 02
    FBA$X0025AUPJP - B07Q6W6LXC @ JY18828 - 02'
    由station_name$sellersku$fnsku-asin@erpsku 
    """
    erpsku_info['seller_sku'] = erpsku_info['标识'].apply(lambda x: x.split('$')[1])
    erpsku_info['asin'] = erpsku_info['标识'].apply(lambda x: x.split('$')[2][11:21])
    erpsku_info.rename(columns={'erp_sku': 'erpsku'}, inplace=True)
    erpsku_info = erpsku_info[['erpsku', 'asin', 'seller_sku']]
    # 将erpsku_info存储到redis中
    now_datetime = datetime.now().strftime('%Y-%m-%d_%H')
    erpsku_redis_key = 'erpsku_info_{}'.format(now_datetime)
    # 删除其他的键
    conn_redis = Redis_Store(decode_responses=True, db=0)
    redis_db0_keys = conn_redis.keys()
    erpsku_redis_key_sign = 'erpsku_info'
    [conn_redis.delete(key) for key in redis_db0_keys if erpsku_redis_key_sign in key]
    conn_redis = Redis_Store(decode_responses=False, db=0)
    conn_redis.redis_upload_df(erpsku_redis_key, erpsku_info)
    conn_redis.close()


def load_sku_erpsku_today_from_redis():
    # erpsku信息存储在redis中的键是以 erpsku_info_日期_小时
    conn_redis = Redis_Store(decode_responses=True, db=0)
    redis_db0_keys = conn_redis.keys()
    erpsku_redis_key_sign = 'erpsku_info'
    now_date = datetime.now().strftime('%Y-%m-%d')
    erpsku_today_key = [key for key in redis_db0_keys if
                        (erpsku_redis_key_sign in key) and (now_date in key)]
    if erpsku_today_key:
        erpsku_exist_key = erpsku_today_key[0]
    else:
        conn_redis.close()
        return pd.DataFrame()
    conn_redis.close()
    conn_redis = Redis_Store(decode_responses=False, db=0)
    erpsku_info = conn_redis.redis_download_df(erpsku_exist_key)
    conn_redis.close()
    return erpsku_info


def load_sku_erpsku():
    """
    加载sku与erpsku的关系表:
        需要考虑到解码
    Returns:pd.DataFrame
    """
    # 从redis中加载erpsku和seller sku对应信息
    erpsku_info = load_sku_erpsku_today_from_redis()
    if erpsku_info.empty:
        # 从公司的服务器加载erpsku信息到redis中
        trans_erpsku_info_from_sql_2_redis()
        erpsku_info = load_sku_erpsku_today_from_redis()
    return erpsku_info


# 打印台字体颜色设置
def print_color(message, text_color='red', background_color=None, underline=False):
    """
    显示颜色的格式：
        \033[显示方式;字体色;背景色m ...... \033[0m
    显示颜色参数:
            显示方式    效果            字体色        背景色    颜色描述
            0          终端默认设置       30           40       黑色
            1          高亮显示          31            41      红色
            4          使用下划线        32            42      绿色
            5          闪烁             33            43      黄色
            7          反白显示         34            44       蓝色
            8          不可见           35            45       紫红色
                                       36            46       青蓝色
                                       37            47       白色
    将颜色数字用英文代替
        黑色：black
        红色:red
        绿色:green
        黄色:yellow
        蓝色:blue
        紫红色:mauve
        青蓝色:cyan
        白色:white

    Args:
        underline :bool  default  False
            显示样式(有无下划线)
        message:str
             需要打印的信息
        text_color : str or None default None
            文字颜色
        background_color : str or None default None
            文字背景颜色

    Returns:None

    """

    # 文本颜色字典
    text_color_dict = {'black': 30, 'red': 31, 'green': 32, 'yellow': 33, 'blue': 34, 'mauve': 35, 'cyan': 36,
                       'white': 37}
    # 文本背景颜色字典
    background_color_dict = {'black': 40, 'red': 41, 'green': 42, 'yellow': 43, 'blue': 44, 'mauve': 45, 'cyan': 46,
                             'white': 47}

    # 判断数据类型
    if not isinstance(message, str):
        raise TypeError(f'ERROR INPUT.{message} type is not string.')
    if text_color is not None:
        if not isinstance(text_color, str):
            raise TypeError(f'ERROR INPUT.{text_color} type is not string.'
                            f'Please input one of {text_color_dict.keys()}')
    if background_color is not None:
        if not isinstance(background_color, str):
            raise TypeError(f'ERROR INPUT.{background_color} type is not string.'
                            f'Please input one of {background_color_dict.keys()}')

    if underline not in [True, False]:
        raise TypeError(f'ERROR INPUT.{underline} type is not bool.'
                        f'Please input one of True or False')

    if text_color is None:
        text_color = ''
    else:
        text_color = text_color_dict[text_color]
    if background_color is None:
        background_color = ''
    else:
        background_color = background_color_dict[background_color]
    if underline is False:
        underline = 0
    else:
        underline = 4

    show_config = f'{underline};{text_color};{background_color}'
    print(f'\033[{show_config}m{message}\033[0m')


def type_verify(data, data_type):
    """
    检测数据类型是否有效

    Parameters
    ----------
    data :object
        被检测数据
    data_type :object
        被检测数据的数据类型

    Returns
    -------
        bool
        数据类型是否有效
    """
    if isinstance(data, data_type):
        return True
    else:
        raise TypeError(f'param input type is {type(data)} not {data_type}.')


def column_to_datetime_active(df,site,column):
    """
    将ac表中的数据列转换为时间格式
    :param df:
    :param site:
    :param column:
    :return:
    """
    type_verify(df,pd.DataFrame)
    type_verify(site,str)
    type_verify(column,str)
    if site in ["US", "CA"]:
        df[column] = pd.to_datetime(df[column], yearfirst=False, dayfirst=False,errors='coerce')
    elif site in ["JP",'MX','AE','IN']:
        df[column] = pd.to_datetime(df[column], yearfirst=True, dayfirst=False,errors='coerce')
    else:
        df[column] = pd.to_datetime(df[column], yearfirst=False, dayfirst=True,errors='coerce')
    # 过滤掉时区
    df[column] = df[column].apply(
        lambda x: datetime.strftime(x, '%Y-%m-%d %H:%M:%S') if pd.notna(x) else x)
    df[column] = pd.to_datetime(df[column])
    return df


def wrong_type_raise_msg(func_name, variable_name, input_type, default_type):
    """
    当函数的参数的输入类型发生错误时,给的错误提示
    Parameters
    ----------
    func_name :str
        函数名
    variable_name :str
        参数类型输入错误的参数名
    input_type :str
        输入的错误的参数类型
    default_type :str
        需要输入的参数类型

    Returns
    -------
        str:
            错误提示
    """
    return f"函数:{func_name}的参数:{variable_name}的输入类型应该是:{default_type},而输入的类型是:{input_type}."


def is_variables_types_valid(variables, types):
    """
    验证函数中输入参数的类型是否正确
    Parameters
    ----------
    variables : object,list of object
        需要验证的参数名
    types : type,set of type,list of type,list of set
        需要验证的参数的类型
    Returns
    -------
        bool:True,False,None
    """
    func_name = is_variables_types_valid.__name__
    # 输入类型验证
    if not isinstance(variables, (object, list)):
        raise_msg = wrong_type_raise_msg(func_name, 'variables', f'{type(variables)}', 'str or list of str')
        raise TypeError(f'{raise_msg}')
    if not isinstance(types, (type, tuple, list)):
        raise_msg = wrong_type_raise_msg(func_name, 'types', f'{type(variables)}',
                                         'str or set of str or list of str or list of set')
        raise TypeError(f'{raise_msg}')
    # 验证的参数为单个参数,验证的数据类型为单个数据类型或是多个
    if isinstance(variables, object) and isinstance(types, (type, tuple)):
        if not isinstance(variables, types):
            raise_msg = wrong_type_raise_msg(func_name, 'variables', f'{type(variables)}',
                                             'str or set of str')
            raise TypeError(f'{raise_msg}')
        else:
            return True
    if isinstance(variables, list) and isinstance(types, list):
        for variable, _type in zip(variables, types):
            if not isinstance(variable, _type):
                raise_msg = wrong_type_raise_msg(func_name, 'variables', f'{type(variables)}',
                                                 'str or set of str or list of str or list of set')
                raise TypeError(f'{raise_msg}')
        else:
            return True


def loop_func(update_time = 4):
    """
    每天定时启动程序

    Parameters
    ----------
    start_time :int
        开始时间

    Returns
    -------

    """
    def wrapper1(func):
        from functools import wraps
        @wraps(func)
        def wrapper(*args, **kwargs):
            while 1:
                now_hour = datetime.now().hour
                if now_hour == update_time:
                    func(*args, **kwargs)
                    print('休息一个小时.')
                    time.sleep(3600)
                else:
                    now_hour = datetime.now().hour
                    if now_hour > update_time:
                        hour_interval = (24 + update_time) - now_hour
                    else:
                        hour_interval = update_time - now_hour
                    print(f'休息{hour_interval}个小时,{update_time}点再开始.')
                    time.sleep(3600 * hour_interval)
        return wrapper
    return wrapper1


def series_numeric_to_percent(series,points_keep = 2):
    """
    将pd.Series转化为百分
    :param series:
    :return:
    """
    if not isinstance(series,pd.Series):
        return series
    else:
        if points_keep>0:
            return [str(round(value*100,points_keep))+"%" for value in series]
        else:
            return [str(int(value*100))+"%" for value in series]


def all_files(folder, include_str=None):
        """
        获取文件夹下全部文件全路径
        Parameters
        ----------
        folder :

        Returns
        -------

        """
        allFilesPath = []
        for root, dirs, files in os.walk(folder):  # 将os.walk在元素中提取的值，分别放到root（根目录），dirs（目录名），files（文件名）中。
            for file in files:
                if include_str is not None:
                    if include_str.lower() not in file.lower():
                        continue
                    allFilesPath.append(os.path.join(root, file))
                else:
                    allFilesPath.append(os.path.join(root, file))

        return allFilesPath


def standardize_station(station,case='lower'):
    """
    标准化站点名
    :param station: str
        station name
    :param case: bool lower or upper
        station is lower or upper
    :return:
    """
    if (station is None) or (not isinstance(station,str)):
        raise ValueError(f'station type must string')
    if case not in ['lower','upper']:
        raise ValueError(f'case must lower or upper')
    if case == 'lower':
        return station.strip().replace('-','_').replace(' ','_').lower()
    else:
        return station.strip().replace('-','_').replace(' ','_').upper()



def standardize_user_number(user_number,case='lower'):
    """
    规范化员工工号
    :param user_number:
    :return:
    """
    if not isinstance(user_number,(str,int)):
        return
    if isinstance(user_number,int):
        user_number = str(user_number)
    if case not in ['lower', 'upper']:
        raise ValueError(f'case must lower or upper')
    if case == 'lower':
        return user_number.lower()
    else:
        return user_number.upper()



if __name__ == '__main__':
    print(detect_lang('我是猪'))
    print(detect_lang('Iam a pig.'))