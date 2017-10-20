import requests
from bs4 import BeautifulSoup
import time
import json
import redis
import ssl
import threading
import random

ssl._create_default_https_context = ssl._create_unverified_context
session = requests.session()
headers = {'Cookie': 'aliyungf_tc=AQAAAJ+82SAg0QYAMkb43Ehcyxz0vsrD;l_n_c=1;q_c1=597b00e142e64da6a9db36ff16210450|1506666151000|1506666151000; r_cap_id="YzQ0NjExYzYzODZmNGQwNzgwYjk1MzM3NGJhNGVlYmE=|1506666151|357bc144d9e4eb157374079132a0cdee74b6a029"; cap_id="MjAyNzYyY2Y5YmYxNDdhYWIzMGMyZjBkNDdlOWM2OTk=|1506666151|a405bd565a0584d60ca8d16f45dd696491edd837"; z_c0="MS4xeWs1REFnQUFBQUFYQUFBQVlRSlZUYk56OVZuUDJJSVBrNy1kQ0hRRy1BdGtpdk9mREwyNlFBPT0=|1506666163|85d44d27f66d8fcaec382bb130e5fada0f38c3a5"',
           'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:55.0) Gecko/20100101 Firefox/55.0',
           'Accept': 'application/json, text/plain, */*'}


def toRedis():
    pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
    r = redis.Redis(connection_pool=pool)
    print('--------------redis connection success-----------')
    return r


def zhizhu():
    headers = {'Cookie': 'aliyungf_tc=AQAAAJ+82SAg0QYAMkb43Ehcyxz0vsrD; l_n_c=1; q_c1=597b00e142e64da6a9db36ff16210450|1506666151000|1506666151000; r_cap_id="YzQ0NjExYzYzODZmNGQwNzgwYjk1MzM3NGJhNGVlYmE=|1506666151|357bc144d9e4eb157374079132a0cdee74b6a029"; cap_id="MjAyNzYyY2Y5YmYxNDdhYWIzMGMyZjBkNDdlOWM2OTk=|1506666151|a405bd565a0584d60ca8d16f45dd696491edd837"; z_c0="MS4xeWs1REFnQUFBQUFYQUFBQVlRSlZUYk56OVZuUDJJSVBrNy1kQ0hRRy1BdGtpdk9mREwyNlFBPT0=|1506666163|85d44d27f66d8fcaec382bb130e5fada0f38c3a5"',
               'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:55.0) Gecko/20100101F irefox/55.0',
               'Accept': 'application/json, text/plain, */*'}
    url = 'https://www.zhihu.com/'
    rs = session.get(url, headers=headers)
    s = session.get(
        'https://www.zhihu.com/people/qia-la-ma-zuo-fu-61/following', headers=headers)

    soup = BeautifulSoup(s.text)
    with open('html/user.html', 'w', encoding='utf-8') as f:
        f.write(soup.prettify())
        f.close()
    
    pageNo = len(soup.select('.Pagination button'))
    offset = 0
    for x in range(1, pageNo):
        print(x)
        param = {'include': 'data[*].answer_count,articles_count,gender,follower_count,is_followed,is_following,badge[?(type=best_answerer)].topics',
                 'offset': str(offset),
                 'limit': '20'}
        s = session.get(
            'https://www.zhihu.com/api/v4/members/qia-la-ma-zuo-fu-61/followees', params=param, headers=headers)
        myjson = json.loads(s.text)
        myjson = json.dumps(myjson, ensure_ascii=False)
        time.sleep(2)
        with open('html/' + str(x) + '.html', 'w+', encoding='utf-8') as f:
            f.write(myjson)
            f.close()
        time.sleep(1)
        offset = offset + 20


def paraser(pageNo):
    try:
        for x in range(1, pageNo):
            with open('html/' + str(x) + '.html', 'r', encoding='utf-8') as f:
                data = json.load(f)
            for x in range(0, 20):
                listd = data['data'][x]
                ids = listd['id']
                url = listd['url'].replace('api/v4/', '')
                urls = listd['url']
                name = listd['name']
                url_token = listd['url_token']
                follower_count = listd['follower_count']
                headline = listd['headline']
                answer_count = listd['answer_count']
                articles_count = listd['articles_count']
                user = {'name': 'name', 'url': url, 'follower_count': follower_count,
                        'headline': headline, 'answer_count': answer_count, 'answer_count': answer_count}
                r.set(ids, user)
                r.sadd('userurls', urls)
                r.sadd('ids', ids)
                r.sadd('url_token', url_token)
                r.sadd('url_token_1',url_token)
                r.sadd('url_token_my',url_token)
                print(r.get(ids).decode('utf-8'))
    except IndexError:
        pass


def urlIteration(r):
    url_tokens = r.smembers('url_token_1')
    for userurl_token in url_tokens:
        s = session.get('https://www.zhihu.com/people/' +
                        userurl_token.decode('utf-8')+ '/following', headers=headers,verify=False)
        time.sleep(1)
        soup = BeautifulSoup(s.text)
        buttonNo = len(soup.select('.Pagination button'))
        # print(len(soup.select('.Pagination button')))
        if buttonNo > 0:
            if buttonNo > 6:
                pageNo = soup.select('.Pagination button')[5].text
            else:
                pageNo = soup.select('.Pagination button')[buttonNo - 2].text
        else:
            pageNo = 1
        try:
            offset = 0
            for x in range(1, int(pageNo) + 1):
                param = {'include': 'data[*].answer_count,articles_count,gender,follower_count,is_followed,is_following,badge[?(type=best_answerer)].topics',
                     'offset': str(offset),
                     'limit': '20'}
                s = session.get(
                    'https://www.zhihu.com/api/v4/members/' + userurl_token.decode('utf-8') + '/followees', params=param, headers=headers)
                myjson = json.loads(s.text,encoding = 'utf-8')
                for i in range(0, 20):
                    listd = myjson['data'][i]
                    ids = listd['id']
                    url = listd['url'].replace('api/v4/', '')
                    urls = listd['url']
                    name = listd['name']
                    url_token = listd['url_token']
                    follower_count = listd['follower_count']
                    headline = listd['headline']
                    answer_count = listd['answer_count']
                    articles_count = listd['articles_count']
                    user = {'name': name, 'url': url, 'follower_count': follower_count,
                            'headline': headline, 'answer_count': answer_count, 'answer_count': answer_count}
                    r.set(ids, user)
                    r.sadd('userurls', urls)
                    r.sadd('ids', ids)
                    if r.sismember('url_token',url_token):
                        pass
                    else:
                        r.sadd('url_token', url_token)
                        r.sadd('url_token_1',url_token)
                    print(r.get(ids).decode('utf-8'))
                time.sleep(2)
                offset = offset + 20
        except IndexError:
            pass
        session.close()
        r.srem('url_token_1',userurl_token)

def action(arg, r):
     while r.scard('url_token_1') > 0:
        userurl_token = r.spop('url_token_1')
        s = session.get('https://www.zhihu.com/people/' +
                        userurl_token.decode('utf-8') + '/following', headers=headers, verify=False)
        time.sleep(1)
        soup = BeautifulSoup(s.text)
        buttonNo = len(soup.select('.Pagination button'))
        if buttonNo > 0:
            if buttonNo > 6:
                pageNo = soup.select('.Pagination button')[5].text
            else:
                pageNo = soup.select('.Pagination button')[buttonNo - 2].text
        else:
            pageNo = 1
        try:
            offset = 0
            for x in range(1, int(pageNo) + 1):
                param = {'include': 'data[*].answer_count,articles_count,gender,follower_count,is_followed,is_followinll,badge[?(type=best_answerer)].topics',
                         'offset': str(offset),
                         'limit': '20'}
                s = session.get(
                    'https://www.zhihu.com/api/v4/members/' + userurl_token.decode('utf-8') + '/followees', params=param, headers=headers)
                myjson = json.loads(s.text, encoding='utf-8')
                for i in range(0, 20):
                    listd = myjson['data'][i]
                    ids = listd['id']
                    url = listd['url'].replace('api/v4/', '')
                    urls = listd['url']
                    name = listd['name']
                    url_token = listd['url_token']
                    follower_count = listd['follower_count']
                    headline = listd['headline']
                    answer_count = listd['answer_count']
                    articles_count = listd['articles_count']
                    user = {'name': name, 'url': url, 'follower_count': follower_count,
                            'headline': headline, 'answer_count': answer_count, 'answer_count': answer_count}
                    r.set(ids, user)
                    r.sadd('userurls', urls)
                    r.sadd('ids', ids)
                    if r.sismember('url_token', url_token):
                        pass
                    else:
                        r.sadd('url_token', url_token)
                        r.sadd('url_token_1', url_token)
                    print(r.get(ids).decode('utf-8'))
                time.sleep(random.randint(10,30))
                offset = offset + 20
        except (IndexError,Exception) as e:
            if 'IndexError' in repr(e):
                pass
            else:
                print(repr(e))
                r.sadd('url_token_1',userurl_token)
                break
        finally:
            session.close()

if __name__ == '__main__':
    # soup = BeautifulSoup(open('html/user.html',encoding='utf-8'))
    # print(len(soup.select('.Pagination button')))
    r = toRedis()
    # paraser(6)
    # urlIteration(r)
    for i in range(2):
        t = threading.Thread(target=action, args=(i, r))
        t.start()
  
