#!/usr/bin/env python
# coding=utf-8
# author:marmot

import re

# 处理小数点为，的
def num_del(num_str):
    num_one=str(num_str)
    if re.search(',',num_one):
        num_list = num_one.split(',')
        if len(num_list) == 1:
            new_num = float(num_one)
        elif len(num_list) == 2:
            if len(num_list[-1]) > 2:
                new_num = float(num_list[0] + num_list[1])
            else:
                new_num = float(num_list[0] + '.' + num_list[1])
        elif len(num_list) >= 3:
            if len(num_list[-1]) > 2:
                new_num = float(reduce(lambda x,y:x+y,num_list))
            else:
                new_num = float(reduce(lambda x,y:x+y,num_list[0:-1]) + '.' + num_list[-1])
    else:
        new_num = float(num_one)
    return new_num

# 处理千分位为.的
def num_del_point(num_str):
    num_one=str(num_str)
    if re.search('.',num_one):
        num_list = num_one.split('.')
        if len(num_list) == 1:
            new_num = float(num_one)
        elif len(num_list) == 2:
            if len(num_list[-1]) > 2:
                new_num = float(num_list[0] + num_list[1])
            else:
                new_num = float(num_list[0] + '.' + num_list[1])
        elif len(num_list) >= 3:
            if len(num_list[-1]) > 2:
                new_num = float(reduce(lambda x,y:x+y,num_list))
            else:
                new_num = float(reduce(lambda x,y:x+y,num_list[0:-1]) + '.' + num_list[-1])
    else:
        new_num = float(num_str)
    return new_num

# 处理百分数
def num_del_percentage(num_str):
    num_one=str(num_str)
    new_num=num_one.strip('%')
    if re.search(',', str(new_num)):
        return '%0.2f'%(num_del(str(new_num))/100)
    else:
        return  '%0.2f'%(float(new_num)/100)