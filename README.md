# **底层服务**
>广告部服务器底层服务：底层主要是用来处理远程请求的五表,将五表处理成中间文件(pkl),然后去实现各项底层任务,为广告平台提供保障。包括从公司服务器拉取五表,处理五表,生成st/sku表现表等任务。底层服务使用Python3.7来实现。
### **技术简介**
>底层服务是部署在windows(172.16.128.240)。使用Python3.7运行对应脚本管理器等则可以启动任务,后期可以用apscheduler方式来部署。

### **任务介绍**
>底层任务主要分为:api获取文件(api_request_files),接口获取数据(java_interface),五表处理(store_five_files.py),生成st/sku表现表(calc_station_st_and_sku.py),计算月数据(calc_monthly/calc_monthly.py)等。

### **运行脚本**
脚本名称|路径|目的|重要性|重要性分级
:---:|:---:|:---:|:---:|:---:|
api_request_five_reports|api_request_files|请求易佰五表|最|4
api_request_gxj_reports|api_request_files|请求光迅嘉五表|重要|3
api_request_chujin_reports|api_request_files|请求楚晋五表|重要|3
price_dist|price_dist|站点数据概览|一般|2
update_st_info|search_sku_asin|上传st表信息|重要|3
calc_monthly|calc_monthly|计算月数据|重要|3
store_five_files|project|保存五表为pkl|重要|3
calc_station_st_and_sku|project|生成st/sku表现|重要|3
query_station_shipment_monthly|java_interface|获取站点到货信息|一般|2
ad_sku_have_ordered|ad_perf|有订单的sku|一般|2
aliexpreee_account|java_interface|api获取速卖通账号|一般|2
query_get_follow_up_list|java_interface|api获取站点到货|一般|2
request_primary_listing|api_request_files|api获取重点listing|一般|2
sku_map_upload_2_ad_server|search_sku_asin|api获取sku捆绑表|重要|3
clear_station_older_files|simple_task|删除过期文件|重要|3
stations_not_take_over_sales|project|没有接手的站点销售额|一般|2
query_nickname|java_interface|api获取花名|一般|2
query_account_status|java_interface|api获取账号状态|重要|3
query_company_organization|java_interface|api获取公司架构|重要|3
query_account_short_name|java_interface|api获取账号简称|重要|3
account_id_index|java_interface|api获取账号索引|重要|3
api_get_cj_amazon_account|java_interface|api楚晋账号|一般|2
api_walmart_get_account|java_interface|api获取walmart账号|一般|2
store_camp_2_mysql|store_ad_info|保存camp层级信息|重要|3
process_seller_upload_stations_five_files|project|将销售上传的报表保存到五表压缩包中|重要|3


### **文件目录**
ad_zyp:
├─ad_perf # 广告表现
│  ├─ad_sku_have_ordered.py # 有订单的sku。将有订单的sku表现保存到数据库中。
├─api_request_files # api获取文件
│  ├─api_request_ac_ao_reports.py # api请求ac报表,现在已经不用。
│  ├─api_request_chujin_reports.py # **api请求楚晋五表**
│  ├─api_request_cp_br_st_reports.py # api请求cp/st/br报表,现在已经不用。
│  ├─api_request_five_reports.py # **api请求易佰五表**
│  ├─api_request_gxj_reports.py # **api请求光迅嘉五表**
│  ├─api_walmart_getitemperformance.py # api请求walmart账号表现,现在已经不用
│  ├─api_walmart_get_account.py # api请求walmart账号,现在已经不用
│  ├─mongo_con.py # 处理mongodb方法,现在已经不用
│  ├─new_listing_auto_camp.py # 新listing自动广告,现在已经不用
│  ├─public.key # token,勿删
│  ├─remote_sftp_monthly_br.py # api请求月数据br报表,现在已经不用
│  ├─remote_sftp_monthly_cp.py # api请求月数据cp报表,现在已经不用
│  ├─reqeust_other_company_account.py # api请求其他公司账号,现在已经不用
│  ├─request_files_result_daily.py # 远程请求五表结果,现在已经不用
│  ├─request_primary_listing.py # api每日定时获取重点listing
│  ├─test.py # 测试脚本
│  ├─upload_station_overall.py # 站点整体销售概况,现在已经不用
├─apscheduler_app # 定时任务框架,可以开发这个来管理后台全部任务.待开发。
├─auto_upload # 自动上传广告,现在已经不用
├─calc_monthly # **月数据计算**
│  ├─calc_monthly.py # **月数据计算**
├─distributed_request_files # 没有用
├─erpsku_sellersku_kws
│  ├─sellersku_restkws.py # 早期用于计算共享关键词,现在不用
├─five_reports_upload_db # 将五表内容上传到数据库,现在不用
│  ├─active_listing_major_info_upload_init.py # 早期用于将ac信息保存到数据库中,现在不用
├─java_interface #java接口,获取公司数据
│  ├─account_id_index.py # 获取公司账号id和账号 
│  ├─aliexpreee_account.py # 获取aliexpress(速卖通)账号
│  ├─api_get_cj_amazon_account.py # 获取楚晋账号
│  ├─query_account_short_name.py # 获取公司账号简称
│  ├─query_account_status.py # 获取公司账号状态
│  ├─query_company_organization.py # 获取公司的组织架构(广告后台销售权限与这个有关)
│  ├─query_get_follow_up_list.py # 获取跟卖信息
│  ├─query_infringement_keyword.py # 获取侵权信息
│  ├─query_nickname.py # 获取员工花名
│  ├─query_station_shipment_monthly.py # 获取站点到货信息
│  ├─seller_account.py # 获取销售负责站点,已经不用,慎用,感觉不太准,会将销售的权限弄乱。
├─login_user backup # 没有用
├─my_toolkit # 工具箱 大部分和广告后台的工具箱一样
│  ├─access_info_query.py # **销售和广告专员有关方法**
│  ├─change_rate.py # **汇率**
│  ├─chinese_check.py # **中文有关方法**
│  ├─commonly_params.py # 常用变量  
│  ├─conn_db.py # 连接数据库
│  ├─get_time.py # 获取时间
│  ├─init_station_report.py # 初始化报表的df
│  ├─monthly_reports.py # 处理销售上传的月数据方法
│  ├─myZip.py # 处理zip函数
│  ├─my_api.py # api类
│  ├─process_company.py # 处理公司的方法
│  ├─process_files.py # 获取文件的一些属性与方法
│  ├─process_station.py # 处理站点方法
│  ├─public_function.py # 早期的一些混用的公共方法
│  ├─query_frequently_table_info.py # **快速查询数据库表**(Redis+MySQL)
│  ├─sql_write_read.py # 处理MySQL和Redis
│  ├─station_belong.py # 早期用于获取站点方法,后来不用
│  ├─type_verify.py # 函数中输入变量类型判断
├─price_dist
│  ├─price_dict.py # 站点表现概览,后期没有人看,需要维护，有些错误.
├─request_reports_from_api # 通过api获取报表,现在不用
├─search_sku_asin # 获取sku与asin
│  ├─sku_map_upload_2_ad_server.py # 通过接口获取sku捆绑表
│  ├─update_st_info.py # 将st报表信息更新到数据库中
├─sellersku_perf # 将sku表现储存到数据库中,现在不用
│  └─sellersku_perfmance.py
├─simple_task # 简单任务
│  ├─clear_station_older_files.py # 按照日期定时清理文件/文件夹
│  ├─station_brand.py # 获取站点品牌广告,现在不用
│  ├─trans_exchange.py  # 临时脚本,转换汇率列
├─static # 静态文件,用于apscheduler,暂时不用
├─station_ad_sale_perf # 站点整体销售表现,暂时不用
├─store_ad_info # 前期用于将广告各个层级保存到数据库中
│  ├─store_camp_2_mysql.py  # 保存camp层级到数据库
├─temp # 临时脚本
├─templates # 模板静态文件,用于apscheduler,暂时不用
├─to_exe # 将脚本打包为exe
│  ├─api_request_files.py # 用于打包的api请求五表
│  ├─batch_upload.py # 用于打包的批量上传
│  ├─distribution_stations_folder_flow.py # 用于打包的五表分发
│  ├─py2exe_pro.py # 用于月数据上传的打包文件,暂时不用
│  ├─to_exe.py # **打包脚本**
├─venv # 项目依赖包
├─.gitignore # git忽略文件/文件夹
├─calc_station_st_and_sku.py # **计算站点的st/sku表现**
├─daily_tasks.py # **定时任务/脚本管理脚本**,可以转到apscheduler.
├─main.py # 测试的定时任务管理器启动程序,暂时用不上
├─process_one_station_st_sku.py # 用于计算单个站点的st/sku表现
├─process_seller_upload_stations_five_files.py # 将销售上传的缺失五表,更新到五表的压缩包中
├─requirements.txt # 依赖包
├─run_multi_pys.py # **用于记录用于需要启动的脚本**
├─scheduler.txt # 无用
├─scheduler_all_task.py # 无用
├─search_redis.py # redis查询
├─stations_not_take_over_sales.py #没有接手的站点销售额 
├─store_five_files.py # **保存五表为pkl文件**
├─store_five_files_for_faster.py # 不用
├─sum_monthly_impressions.py # 临时任务,用于计算全部广告订单,点击
├─test.py # 测试脚本
├─upload_st.py # 上传st报表中的关键信息,已经不用


### **主要脚本**
>最最最重要的脚本是**api获取五表(api_request_files下三个公司的站点文件获取)**,其次是计算sku/st报表的计算与报表转换为pkl文件,然后就是各类api请求数据等。


### **注意事项**
>0.api请求五表定时任务关系到报表的自动获取,若脚本挂掉了,则公司的大部分销售需要补表,很耗时.
>1.api获取五表中,st/br/cp(三个表)是获取公司服务器(yiduan.yibai-it.com)文件夹下载文件.ao/ac报表是通过api接口获取的。另外,易佰的三个表请求时间在6点以后,楚晋和光迅嘉的在8点以后,因为这样请求日志才是完整的。ac/ao报表尽量在下午5点以后获取即可。
>2.run_multi_pys.py 可以查看需要启动的脚本,但是我将全部脚本分为可以批量启动(daliy_task.py)+需要实时查看效果两种.




