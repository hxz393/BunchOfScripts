# coding=gbk
import re

import requests
from requests.adapters import HTTPAdapter

# �����ַ = {
#     # 'http': 'http://192.168.2.102:808',
#     # 'http': 'http://192.168.2.103:808',
#     'http': 'http://192.168.2.104:808',
# }
С���� = 'tcc; tzWHMELq=gkFrCnQx; tzWHMELq=gkFrCnQx; aby=2; skt=W7A6546okv; skt=W7A6546okv'
����ͷ�� = {
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
    'Cookie': С����,
    # 'Referer': 'https://rarbgprx.org/torrent/78xvu6q',
}
requests.packages.urllib3.disable_warnings()
�Ự = requests.Session()
�Ự.mount('http://', HTTPAdapter(max_retries=3))
�Ự.mount('https://', HTTPAdapter(max_retries=3))
�Ƿ��ַ� = r"[\/\\\:\*\?\"\<\>\|]"
# ʹ��pycharm����Ĵ���
# �Ự.trust_env = False

with open(r'B:\2.�ű�\�½��ı��ĵ�.txt') as file:
    idn = file.read()
�����б� = [i for i in idn.split("\n")]


def ��������(�����б�):
    nnn = 1
    for �����ַ in �����б�:
        # response = �Ự.get(�����ַ, timeout=10, headers=����ͷ��, verify=False, proxies=�����ַ)
        response = �Ự.get(�����ַ, timeout=10, headers=����ͷ��, verify=False)
        if response.status_code == 200:
            ����ԭʼ���� = response.text
            # print(����ԭʼ����)
            xx = ����ԭʼ����.find('/static/20/img/magnet.gif')
            if xx < 0:
                print('��' + str(nnn) + '��������1����: ' + �����ַ)
                break
            else:
                yy = re.findall(r'href="/download.php\?id=(.+?)"', ����ԭʼ����)
                �ļ��� = re.findall(r'&f=(.+)', yy[0])[0]
                �������� = 'https://rarbgprx.org/download.php?id=' + yy[0]

                # torrentfile = �Ự.get(��������, timeout=10, headers=����ͷ��, verify=False, proxies=�����ַ)
                torrentfile = �Ự.get(��������, timeout=10, headers=����ͷ��, verify=False)
                zzz = str(torrentfile.content).find('d8:announce')
                if zzz > 1:
                    with open('B:/2.�ű�/' + �ļ���, 'wb') as f:
                        f.write(torrentfile.content, )
                        print('��' + str(nnn) + '�������������: ' + �����ַ)
                else:
                    print('��' + str(nnn) + '������������,�ﵽ��������: ' + �����ַ)
                    break
        else:
            print('��' + str(nnn) + '��������3����: ' + �����ַ)
            break
        nnn = nnn + 1


def ��ȡ����(�����б�):
    nnn = 1
    for �����ַ in �����б�:
        # response = �Ự.get(�����ַ, timeout=10, headers=����ͷ��, verify=False, proxies=�����ַ)
        response = �Ự.get(�����ַ, timeout=10, headers=����ͷ��, verify=False)
        if response.status_code == 200:
            ����ԭʼ���� = response.text
            # print(����ԭʼ����)
            xx = ����ԭʼ����.find('/static/20/img/magnet.gif')
            if xx < 0:
                print('��' + str(nnn) + '��������1����: ' + �����ַ)
                break
            else:
                # print(����ԭʼ����)
                ���� = re.findall(r'<a href="(magnet:.+?)">', ����ԭʼ����)[0]
                # print(����)
                with open(r'B:\2.�ű�\���.txt', "a", encoding="utf8") as �ļ�:
                    д������ = [���� + '\n']
                    �ļ�.writelines(д������)
                    print('��' + str(nnn) + '������ץȡ���: ' + �����ַ)
        else:
            print('��' + str(nnn) + '��������3����: ' + �����ַ)
            break
        nnn = nnn + 1


��������(�����б�)
��ȡ����(�����б�)
