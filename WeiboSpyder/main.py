#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from spider import *

def main():
    try:
        user_1 = Weibo(2369507040, "2018-01-01")      
        user_2 = Weibo("2826785055", 30)
        user_1.start()  # 爬取微博信息
        user_2.start()  # 爬取微博信息
    except Exception as e:
        logger.exception(e)
        
if __name__ == '__main__':
    main()