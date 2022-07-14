from time import time
import requests
import json
from bs4 import BeautifulSoup
from pymysql import cursors, connect
from argparse import ArgumentParser
from IPython import embed

def parse_response(response):
    parsed = BeautifulSoup(response.text, 'lxml')
    hot_items = parsed.find_all(class_='HotItem')
    data = []
    for item in hot_items:
        data.append({
                "rank" : item.find(class_="HotItem-rank").string if item.find(class_="HotItem-rank") else None,
                "title" : item.find(class_="HotItem-title").string if item.find(class_="HotItem-title") else None,
                "excerpt" : item.find(class_="HotItem-excerpt").string if item.find(class_="HotItem-excerpt") else None,
                "metrics" : item.find(class_="HotItem-metrics").get_text().split()[0] if item.find(class_="HotItem-metrics") else None
            })
    return data
        
def to_database(data, mysql_config):
    d = int(time())
    with connect(**mysql_config["info"], cursorclass=cursors.DictCursor, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_config['database']}")
            cursor.execute(f"USE {mysql_config['database']}")
            cursor.execute(f"CREATE TABLE IF NOT EXISTS `{d}` (`rank` INT, `title` LONGTEXT, `excerpt` LONGTEXT, `metrics` INT)")
            for item in data:
                cursor.execute(f"INSERT INTO `{d}` VALUES (%s, %s, %s, %s)", (item['rank'], item['title'], item['excerpt'], item['metrics']))
        
def to_json(data):
    d = int(time())
    with open(f"{d}.json", "w") as fp:
        json.dump(data, fp, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    try:
        parser = ArgumentParser(description='zhihu board crawler')
        parser.add_argument('--mysql', help='save to mysql server', action="store_true")
        parser.add_argument('--json', help='save as json', action="store_true")
        args = parser.parse_args()
        if not args.mysql and not args.json:
            print("Please choose a way to save the result!")
        else:
            with open("httpheaders.json", "r") as fp:
                headers = json.load(fp)
            response = requests.get("https://www.zhihu.com/hot", headers=headers)
            data = parse_response(response)
            if args.mysql:
                with open("mysqlconfig.json", "r") as fp:
                    mysql_config = json.load(fp)
                to_database(data, mysql_config)
                print("Done.")
            elif args.json:
                to_json(data)
                print("Done.")
    except Exception as e:
        print(e)
        embed()