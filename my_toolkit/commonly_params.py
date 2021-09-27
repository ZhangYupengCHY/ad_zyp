"""
常用常量
"""

"""
站点常量
"""


# 公司erp ip
COMPANY_ERP_IP = '121.37.29.133'


# 站点报表类型
# 广告报表,active listing,search term,all order,all listing,business report,产品报表
station_report_type = ['cp','ac','st','ao','al','br','bd']



"""
redis key
"""

# 五表在redis中的关键词 默认 db=0
five_files_redis_sign = 'FIVE_FILES_KEYS_SAVE'

# camp层级处理的redis键
station_cp_camp_degree_key = 'STATION_CAMP'

# sku/st表现的redis键
STATION_SKU_ST_REDIS_KEY = 'station_st_sku'

# sku/erpsku/asin在redis中的关键词 默认 db=0
erpsku_redis_sign = 'erpsku_info'

# 站点节点
site_web = {'US': 'Amazon.com', 'CA': 'Amazon.ca', 'FR': 'Amazon.fr', 'UK': 'Amazon.co.uk', 'DE': 'Amazon.de',
            'ES': 'Amazon.es', 'IT': 'Amazon.it', 'JP': 'Amazon.jp', 'MX': 'Amazon.com.mx', 'IN': 'Amazon.in',
            'AU': 'Amazon.com.au','AE':'Amazon.ae','NL':"Amazon.nl"}

