#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
港交所新上市信息爬虫
爬取 https://www2.hkexnews.hk 的IPO信息
"""

import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime


def crawl_hk_ipo():
    """爬取港交所主板新上市信息"""
    
    url = "https://www2.hkexnews.hk/New-Listings/New-Listing-Information/Main-Board?sc_lang=zh-HK"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        response.encoding = 'utf-8'
    except requests.RequestException as e:
        print(f"请求失败: {e}")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 查找表格
    table = soup.find('table')
    if not table:
        print("未找到表格")
        return []
    
    ipo_list = []
    
    # 获取所有数据行（跳过表头）
    rows = table.find_all('tr')[1:]  # 跳过表头行
    
    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 5:
            continue
        
        # 提取股份代号
        stock_code = cols[0].get_text(strip=True)
        
        # 提取股份名称
        stock_name = cols[1].get_text(strip=True)
        
        # 提取新上市公告链接
        listing_notice_link = ""
        listing_notice_a = cols[2].find('a')
        if listing_notice_a and listing_notice_a.get('href'):
            listing_notice_link = listing_notice_a.get('href')
        
        # 提取招股章程链接
        prospectus_link = ""
        prospectus_a = cols[3].find('a')
        if prospectus_a and prospectus_a.get('href'):
            prospectus_link = prospectus_a.get('href')
        
        # 提取股份配发结果链接
        allotment_link = ""
        allotment_a = cols[4].find('a')
        if allotment_a and allotment_a.get('href'):
            allotment_link = allotment_a.get('href')
        
        ipo_info = {
            "股份代号": stock_code,
            "股份名称": stock_name,
            "新上市公告": listing_notice_link,
            "招股章程": prospectus_link,
            "股份配发结果": allotment_link
        }
        
        ipo_list.append(ipo_info)
    
    return ipo_list


def save_results(ipo_list, filename="ipo_results.json"):
    """保存爬取结果到JSON文件"""
    
    result = {
        "爬取时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "数据来源": "https://www2.hkexnews.hk/New-Listings/New-Listing-Information/Main-Board?sc_lang=zh-HK",
        "IPO列表": ipo_list
    }
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"结果已保存到 {filename}")


def print_results(ipo_list):
    """打印爬取结果"""
    
    print("\n" + "=" * 80)
    print("港交所主板新上市信息")
    print("=" * 80)
    
    for i, ipo in enumerate(ipo_list, 1):
        print(f"\n【{i}】{ipo['股份名称']} ({ipo['股份代号']})")
        print("-" * 40)
        
        if ipo['新上市公告']:
            print(f"  📄 新上市公告: {ipo['新上市公告']}")
        
        if ipo['招股章程']:
            print(f"  📋 招股章程: {ipo['招股章程']}")
        
        if ipo['股份配发结果']:
            print(f"  📊 股份配发结果: {ipo['股份配发结果']}")
    
    print("\n" + "=" * 80)
    print(f"共找到 {len(ipo_list)} 条IPO信息")
    print("=" * 80)


def main():
    print("开始爬取港交所新上市信息...")
    
    ipo_list = crawl_hk_ipo()
    
    if ipo_list:
        print_results(ipo_list)
        save_results(ipo_list)
    else:
        print("未获取到IPO信息")


if __name__ == "__main__":
    main()





