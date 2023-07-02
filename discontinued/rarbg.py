# coding=gbk
import re

import requests
from requests.adapters import HTTPAdapter

# 代理地址 = {
#     # 'http': 'http://192.168.2.102:808',
#     # 'http': 'http://192.168.2.103:808',
#     'http': 'http://192.168.2.104:808',
# }
小甜甜 = 'tcc; tzWHMELq=gkFrCnQx; tzWHMELq=gkFrCnQx; aby=2; skt=W7A6546okv; skt=W7A6546okv'
请求头部 = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7,en-GB;q=0.6',
    'Host': 'rarbgprx.org',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'Cookie': 小甜甜,
    # 'Referer': 'https://rarbgprx.org/torrent/78xvu6q',
}
requests.packages.urllib3.disable_warnings()
会话 = requests.Session()
会话.mount('http://', HTTPAdapter(max_retries=3))
会话.mount('https://', HTTPAdapter(max_retries=3))
非法字符 = r"[\/\\\:\*\?\"\<\>\|]"
# 使用pycharm中配的代理
# 会话.trust_env = False

with open(r'B:\2.脚本\新建文本文档.txt') as file:
    idn = file.read()
链接列表 = [i for i in idn.split("\n")]


def 下载种子(链接列表):
    nnn = 1
    for 请求地址 in 链接列表:
        # response = 会话.get(请求地址, timeout=10, headers=请求头部, verify=False, proxies=代理地址)
        response = 会话.get(请求地址, timeout=10, headers=请求头部, verify=False)
        if response.status_code == 200:
            返回原始数据 = response.text
            # print(返回原始数据)
            xx = 返回原始数据.find('/static/20/img/magnet.gif')
            if xx < 0:
                print('第' + str(nnn) + '个链接有1问题: ' + 请求地址)
                break
            else:
                yy = re.findall(r'href="/download.php\?id=(.+?)"', 返回原始数据)
                文件名 = re.findall(r'&f=(.+)', yy[0])[0]
                下载链接 = 'https://rarbgprx.org/download.php?id=' + yy[0]

                # torrentfile = 会话.get(下载链接, timeout=10, headers=请求头部, verify=False, proxies=代理地址)
                torrentfile = 会话.get(下载链接, timeout=10, headers=请求头部, verify=False)
                zzz = str(torrentfile.content).find('d8:announce')
                if zzz > 1:
                    with open('B:/2.脚本/' + 文件名, 'wb') as f:
                        f.write(torrentfile.content, )
                        print('第' + str(nnn) + '个链接下载完成: ' + 请求地址)
                else:
                    print('第' + str(nnn) + '个链接有问题,达到下载限制: ' + 请求地址)
                    break
        else:
            print('第' + str(nnn) + '个链接有3问题: ' + 请求地址)
            break
        nnn = nnn + 1


def 获取磁链(链接列表):
    nnn = 1
    for 请求地址 in 链接列表:
        # response = 会话.get(请求地址, timeout=10, headers=请求头部, verify=False, proxies=代理地址)
        response = 会话.get(请求地址, timeout=10, headers=请求头部, verify=False)
        if response.status_code == 200:
            返回原始数据 = response.text
            # print(返回原始数据)
            xx = 返回原始数据.find('/static/20/img/magnet.gif')
            if xx < 0:
                print('第' + str(nnn) + '个链接有1问题: ' + 请求地址)
                break
            else:
                # print(返回原始数据)
                磁链 = re.findall(r'<a href="(magnet:.+?)">', 返回原始数据)[0]
                # print(磁链)
                with open(r'B:\2.脚本\结果.txt', "a", encoding="utf8") as 文件:
                    写入内容 = [磁链 + '\n']
                    文件.writelines(写入内容)
                    print('第' + str(nnn) + '个链接抓取完成: ' + 请求地址)
        else:
            print('第' + str(nnn) + '个链接有3问题: ' + 请求地址)
            break
        nnn = nnn + 1


下载种子(链接列表)
获取磁链(链接列表)
