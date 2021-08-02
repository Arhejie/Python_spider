import requests
import re
import logging
import pymongo
import multiprocessing
from pyquery import PyQuery
from urllib.parse import urljoin


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s: %(message)s")

BASE_URL = 'https://static1.scrape.cuiqingcai.com'
TOTAL_PAGE = 10

# 将数据保存在数据库中
MONGO_CONNECTION_STARING = 'mongodb://localhost:27017'
MONGO_DB_NAME = 'movies'
MONGO_COLLECTION_NAME = 'movies'
client = pymongo.MongoClient(MONGO_CONNECTION_STARING)
db = client[MONGO_DB_NAME]
collection = db[MONGO_COLLECTION_NAME]


def scrape_page(url):
    """
    获取指定URL的HTML代码
    :param url:
    :return: HTML
    """
    logging.info("scraping %s...", url)
    try:
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            return response.text
        logging.error("Get invalid status code %s while scraping %s", response.status_code, url)
    except requests.RequestException:
        logging.error("Error occurred while scraping %s", url, exc_info=True)


def scrape_index(page):
    """获取每页的HTML代码"""
    index_url = f'{BASE_URL}/page/{page}'  # 拼接URL
    return scrape_page(index_url)


def parse_index(html):
    """
    解析HTML代码
    :param html:
    :return: 详情页URL
    """
    doc = PyQuery(html)
    links = doc('.el-card .name')
    for link in links.items():
        href = link.attr('href')
        detail_url = urljoin(BASE_URL, href)  # 拼接得到电影详情页的URL
        logging.info("Get detail url %s", detail_url)
        yield detail_url


def scrape_detail(url):
    """
    获取详情页的源代码，之所以还要定义该函数，是为了逻辑清晰，之后方便改动，更加灵活
    :param url:
    :return: 电影详情页的HTML
    """
    return scrape_page(url)


def parse_detail(html):
    """
    解析电影的详情页
    :param html:
    :return: 电影封面、电影名称、分类、上映时间、电影简介、评分
    """
    doc = PyQuery(html)

    # 获取封面的URL
    cover = doc("img.cover").attr('src')

    # >选择器是匹配指定元素的一级子元素，而不是所有的后代元素
    name = doc("a > h2").text()

    # 获取标签列表
    categories = [item.text() for item in doc('.categories button span').items()]

    # 使用了伪类选择器，选取info类下包含’上映‘文本的节点，并返回该节点内容
    published_time = doc('.info:contains(上映)').text()

    # 获取节点和正则匹配顺利再继续
    if published_time and re.search(r'\d{4}-\d{2}-\d{2}', published_time):
        published_time = re.search(r'\d{4}-\d{2}-\d{2}', published_time).group()  # 上映时间后面的’上映‘二字不要
    else:
        published_time = None

    # 获取电影简介
    drama = doc('.drama p').text()

    # 获取电影评分
    score = doc("p.score").text()  # 两个类选择器之间没有空格形成多类选择器,有空格形成后代选择器
    score = float(score) if score else None

    return {
        'cover': cover,
        'name': name,
        'categories': categories,
        'published_time': published_time,
        'drama': drama,
        'score': score,
    }


def save_data(data):
    """
    将数据保存在MongoDB中
    :param data:
    """
    condition = {'name': data.get('name')}  # 第一个参数是查询条件
    collection.update_one(condition, {'$set': data}, upsert=True)  # 设置存在即更新，不存在即插入


def main(page):
    index_html = scrape_index(page)  # 获取page页的HTML代码
    detail_urls = parse_index(index_html)  # 获得详情页URL,每页有10个

    # 开始逐个解析详情页URL
    for detail_url in detail_urls:
        detail_html = scrape_detail(detail_url)  # 获取详情页的HTML代码
        data = parse_detail(detail_html)
        logging.info("Get detail data.")
        logging.info("Saving data to mongodb.")
        save_data(data)  # 数据保存
        logging.info("Data saved successfully.")


if __name__ == "__main__":
    pool = multiprocessing.Pool()  # 创建进程池,无参数时使用所有CPU的核，Pool(processes=3)
    pages = range(1, TOTAL_PAGE+1)
    pool.map(main, pages)  # 相当于给main方法传参，page = pages
    pool.close()
    pool.join()
