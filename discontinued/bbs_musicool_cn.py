import os
import re

import requests
from lxml import etree
from retrying import retry

###################配置######################
账户Cookie = 'Iy5o_96af_seccodecSAhDig724JI=760.129f29f1343717ad85; Iy5o_96af_smile=1D1; Iy5o_96af_seccodecSAn1OZ2FQf8=899.0c9bea86069e3da39c; Iy5o_96af_home_diymode=1; Iy5o_96af_seccodecSANqiEhdd3D=2363.21e774a6714cf75380; Iy5o_96af_saltkey=Ss4sG5sf; Iy5o_96af_lastvisit=1647442433; Iy5o_96af_seccodecSARWzTZW67u=2163.5345198c0d4957fe08; Iy5o_96af_auth=2e0cZipW0DXtPWVuRHS4jY8x0jhu%2FrjosOCSNXfzoRWZ%2FQHTXROxh9Yii35rYRpmOVxo07P6pbdF7wjbnLUTcCDWtJu5; Iy5o_96af_lastcheckfeed=2145788%7C1647446067; Iy5o_96af_sid=Hh6WIi; Iy5o_96af_lip=113.246.107.81%2C1649568691; Iy5o_96af_ulastactivity=1b53CVsrGJp0s9DoQLa60vzeBiz7y5zfnIcvCiETUqcjPIXycfCI; Iy5o_96af_nofavfid=1; Iy5o_96af_st_p=2145788%7C1649606440%7Cf008831309f27d13b1ba4bc8beeea490; Iy5o_96af_viewid=tid_685186; Iy5o_96af_st_t=2145788%7C1649606649%7C6bc9b6d927f7134f7658e48daaf8f01d; Iy5o_96af_forum_lastvisit=D_3_1649605245D_5_1649606050D_111_1649606051D_140_1649606054D_147_1649606100D_146_1649606119D_98_1649606132D_100_1649606150D_41_1649606292D_152_1649606368D_148_1649606401D_144_1649606422D_102_1649606487D_97_1649606649; Iy5o_96af_visitedfid=97D102D144D148D152D41D100D98D146D147; Iy5o_96af_lastact=1649606711%09forum.php%09ajax'

检测文本 = r'B:\2.脚本\新建文本文档.txt'
输出文本 = r'B:\2.脚本\输出.txt'

请求头部 = {
    'Host': 'bbs.musicool.cn',
    'Connection': 'keep-alive',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-User': '?1',
    'Sec-Fetch-Dest': 'document',
    'Referer': 'https://bbs.musicool.cn',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7,en-GB;q=0.6,ru;q=0.5',
    'Cookie': 账户Cookie
}
requests.packages.urllib3.disable_warnings()

替换内容 = ['本帖隐藏的内容(VIP权限查看)', '链接：', '提取码：', '链接:', '提取码:', '[url]', '百度下载  ', '  密码 ']


###################抓取######################
@retry(stop_max_attempt_number=5, wait_random_min=100, wait_random_max=1200)
def 获取网页内容(链接):
    响应内容 = requests.get(url=链接, headers=请求头部, timeout=15, verify=False, allow_redirects=True).text
    解析内容 = etree.HTML(响应内容)
    # print(响应内容)
    # print(响应内容.count('pan.baidu.com'))
    原始百度网盘列表 = re.findall(r'https://pan.baidu.com/s/.{23}', 响应内容)
    原始百度网盘集合 = set(原始百度网盘列表)
    原始百度网盘数量 = len(set(re.findall(r'https://pan.baidu.com/s/.{7}', 响应内容)))
    # print(原始百度网盘集合)
    # print(原始百度网盘数量)

    帖子标题 = 解析内容.xpath('//*[@id="thread_subject"]/text()')[0]
    帖子标题 = '[{}] {}'.format(链接, 帖子标题)
    隐藏内容 = 解析内容.xpath('//div[@class="showhide"]')[0].xpath("string(.)")
    for 替换 in 替换内容:
        隐藏内容 = 隐藏内容.replace(替换, ' ').strip()
    隐藏内容 = re.sub(r'\r\n\s*(\s\w{4})', r'\1', 隐藏内容)
    隐藏内容 = re.sub(r'--来自百度网盘超级会员V\d的分享', r'', 隐藏内容).strip()
    隐藏内容 = 隐藏内容.replace("?pwd=", ' ').strip()
    # print(隐藏内容)
    # 隐藏内容 = re.sub(r'^(https.{42})\s', r'\1',隐藏内容)
    获取百度网盘数量 = len(re.findall(r'https://pan.baidu.com/s/.{7}', 隐藏内容))
    # print(获取百度网盘数量)
    if 原始百度网盘数量 == 获取百度网盘数量:
        返回列表 = {'标题': 帖子标题, '下载': 隐藏内容}
        return (返回列表)
    elif 原始百度网盘数量 == 1 and 获取百度网盘数量 == 0:
        返回列表 = {'标题': 帖子标题, '下载': list(原始百度网盘集合)[0] + ' ' + 隐藏内容}
        return (返回列表)
    else:
        print('原始数量:' + str(原始百度网盘数量) + ', 实际数量:' + str(获取百度网盘数量) + '. 需要手动处理链接:' + 帖子标题)
        os._exit(1)


###################运行######################

with open(检测文本) as file:
    idn = file.read()
链接列表 = [i for i in idn.split("\n")]

for 链接 in 链接列表:
    返回列表 = 获取网页内容(链接)
    with open(输出文本, "a", encoding='utf-8') as 文件:
        写入内容 = [返回列表['标题'] + '\n' + 返回列表['下载'] + '\n' + '*' * 52 + '\n']
        文件.writelines(写入内容)
        print('抓取完成:' + 返回列表['标题'])
