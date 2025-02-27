import glob
import os
import re
import shutil
import stat

import requests
from lxml import etree
from retrying import retry

整理目录 = r'B:\0.整理\手动整理'
# 整理目录 = r'E:\没有nfo'

没有nfo目录 = r'B:\0.整理\没有nfo'
os.makedirs(没有nfo目录, exist_ok=True)

原始目录列表 = []
没有nfo列表 = []
有nfo列表 = []

requests.packages.urllib3.disable_warnings()
非法字符 = r"[\/\\\:\*\?\"\<\>\|]"


def 去除权限(func, 目标路径, _):
    os.chmod(目标路径, stat.S_IWRITE)
    func(目标路径)


############################################ 读取nfo ############################################
def 读取nfo文件内容(nfo文件路径):
    with open(nfo文件路径, 'rb') as 文件对象:
        检测文本内容 = 文件对象.read()
        print(nfo文件路径)
        print(str(检测文本内容).replace('\\r\\n', '\\n'))


def 读取nfo(整理目录):
    nfo文件路径列表 = glob.glob(os.path.join(整理目录, '**', '*.nfo'), recursive=True)
    # print(nfo文件路径列表)
    for nfo文件路径 in nfo文件路径列表:
        读取nfo文件内容(nfo文件路径)


############################################ 整理音乐节 ############################################
def get_subdirectories(directory):
    subdirectories = [entry.path for entry in os.scandir(directory) if entry.is_dir()]
    return subdirectories


def 整理音乐节(path):
    # 获取子目录的子目录列表
    sub_subdirectories = get_subdirectories(path)
    # 去除直接子目录，只保留子目录中的子目录列表
    sub_sub_subdirectories = []
    for sub_directory in sub_subdirectories:
        sub_sub_subdirectories.extend(get_subdirectories(sub_directory))

    正则表达式列表 = [
        re.compile(r'20\d\d-(.+?)_B2B'),
        re.compile(r'20\d\d-(.+?)_Warmup_Mix'),
        re.compile(r'20\d\d-(.+?)_Live'),
        re.compile(r'20\d\d_-_(.+?)_Live'),
    ]

    # 处理子目录中的子目录名
    for sub_sub_subdirectory in sub_sub_subdirectories:
        原始目录 = sub_sub_subdirectory
        文件夹完整名 = os.path.basename(原始目录)
        print(f'原始目录：{文件夹完整名}')

        for i, 正则表达式 in enumerate(正则表达式列表, start=1):
            匹配结果 = 正则表达式.findall(文件夹完整名)
            if 匹配结果:
                艺术家 = 匹配结果[0]
                艺术家 = 艺术家.replace('_', ' ')
                print(f'命中{i}')
                目标目录 = os.path.join(r'B:\2.脚本', 艺术家.strip(), 文件夹完整名)
                try:
                    shutil.move(原始目录, 目标目录)
                    # os.rename(原始目录, 目标目录)
                    print(f'移动到：{目标目录}')
                    print('*' * 66)
                except Exception as e:
                    print(f'移动文件夹时发生错误: {str(e)}')
        else:
            print('没有命中.')
            print('*' * 66)


@retry(stop_max_attempt_number=120, wait_random_min=100, wait_random_max=1200)
def 读取链接(整理目录):
    整理目录列表 = os.listdir(整理目录)
    for 整理目录列表单个 in 整理目录列表:
        原始目录 = os.path.join(整理目录, 整理目录列表单个)
        原始目录列表.append(原始目录)
        for 根目录, 目录列表, 文件列表 in os.walk(原始目录):
            for 文件 in 文件列表:
                if 文件.split('.')[-1].lower() == 'nfo':
                    有nfo列表.append(根目录)
                    nfo文件路径 = os.path.join(根目录, 文件)
                    with open(nfo文件路径, 'rb') as 文件:
                        检测文本内容 = 文件.read()

                        厂牌 = ''
                        if re.findall(r'(https://(?:www.)?deezer.com(?:/\w{2})?/album/\d+)', str(检测文本内容)):
                            链接 = re.findall(r'(https://(?:www.)?deezer.com(?:/\w{2})?/album/\d+)', str(检测文本内容))[0]
                            print('找到deezer:' + str(链接))
                            响应内容 = requests.get(url=链接, timeout=15, verify=False, allow_redirects=True).text
                            # print(响应内容)
                            解析内容 = etree.HTML(响应内容)
                            if 解析内容.xpath('//*[@class="naboo-album-label label"]/dl[1]/dt[1]/text()'):
                                厂牌带日期 = 解析内容.xpath('//*[@class="naboo-album-label label"]/dl[1]/dt[1]/text()')[0]
                                厂牌带日期 = 厂牌带日期.strip()
                                # print(厂牌带日期)
                                厂牌 = re.findall(r'\d{4}\s\|\s(.+)', str(厂牌带日期))[0]
                                # print(厂牌)
                            else:
                                print('页面不存在')
                        elif re.findall(r'(https://www.beatport.com/release/(?:.+?)/\d+)', str(检测文本内容)):
                            链接 = re.findall(r'(https://www.beatport.com/release/(?:.+?)/\d+)', str(检测文本内容))[0]
                            print('找到beatport:' + str(链接))
                            响应内容 = requests.get(url=链接, timeout=15, verify=False, allow_redirects=True).text
                            # print(响应内容)
                            if re.findall(r'"recordLabel".+"name":\s"(.+?)"},\s"catalogNumber":', str(响应内容)):
                                厂牌 = re.findall(r'"recordLabel".+"name":\s"(.+?)"},\s"catalogNumber":', str(响应内容))[0]
                            else:
                                print('页面不存在')
                        elif re.findall(r'(https://www.junodownload.com/products/(?:.+?/)?(?:.+?)/)', str(检测文本内容)):
                            链接 = re.findall(r'(https://www.junodownload.com/products/(?:.+?/)?(?:.+?)/)', str(检测文本内容))[0]
                            print('找到junodownload:' + str(链接))
                            链接 = re.sub(r'\s+\\xdb\\xdb\\xdb\\xdb(\\r)*\\n\s\\xdb\\xdb\\xdb\\xdb\s+', "", 链接)
                            # print(链接)
                            响应内容 = requests.get(url=链接, timeout=15, verify=False, allow_redirects=True).text
                            # print(响应内容)
                            解析内容 = etree.HTML(响应内容)
                            if 解析内容.xpath('//*[@class="product-label"]/a[1]/text()'):
                                厂牌 = 解析内容.xpath('//*[@class="product-label"]/a[1]/text()')[0].strip()
                                # print(厂牌)
                            else:
                                print('页面不存在')
                        elif re.findall(r'(https://www.qobuz.com/\w\w-\w\w/album/(?:.+?)/(?:\S+))', str(检测文本内容)):
                            链接 = re.findall(r'(https://www.qobuz.com/\w\w-\w\w/album/(?:.+?)/(?:\S+))', str(检测文本内容))[0]
                            链接 = re.sub(r'\\n', "", 链接)
                            链接 = re.sub(r'\\s', "", 链接)
                            print('找到qobuz:' + str(链接))

                            # print(链接)
                            响应内容 = requests.get(url=链接, timeout=15, verify=False, allow_redirects=True).text
                            # print(响应内容)
                            if re.findall(r'Label:\s\s<a\sclass="album-about__item\salbum-about__item--link"\shref="/.+?">(.+?)</a>', str(响应内容)):
                                厂牌 = re.findall(r'Label:\s\s<a\sclass="album-about__item\salbum-about__item--link"\shref="/.+?">(.+?)</a>', str(响应内容))[0]
                            else:
                                print('页面不存在')
                        else:
                            print('找不到URL链接:' + 根目录)
                    if 厂牌:
                        厂牌 = re.sub(r'\\r', r"", 厂牌)
                        厂牌 = re.sub(r'\\n', r"", 厂牌)
                        厂牌 = re.sub(非法字符, "-", 厂牌)
                        目标目录 = os.path.join(r'B:\2.脚本', 厂牌.strip(), 整理目录列表单个)
                        来源目录 = 根目录
                        print(厂牌.strip())
                        print(来源目录)
                        print(目标目录)
                        shutil.move(来源目录, 目标目录)
                        厂牌 = ''
                        print('++++++++++++++++++++++')
                        break
                    else:
                        # shutil.move(根目录, 没有nfo目录)
                        厂牌 = ''
                        break
    return 有nfo列表


def 整理nfo(整理目录):
    整理目录列表 = os.listdir(整理目录)
    for 整理目录列表单个 in 整理目录列表:
        原始目录 = os.path.join(整理目录, 整理目录列表单个)
        原始目录列表.append(原始目录)
        for 根目录, 目录列表, 文件列表 in os.walk(原始目录):
            for 文件 in 文件列表:
                if 文件.split('.')[-1].lower() == 'nfo':
                    有nfo列表.append(根目录)
                    nfo文件路径 = os.path.join(根目录, 文件)
                    with open(nfo文件路径, 'rb') as 文件:
                        检测文本内容 = 文件.read()
                        # print(str(检测文本内容))
                        # break

                        厂牌 = ''
                        检测文本内容 = str(检测文本内容).replace('\\r\\n', '\\n')
                        正则表达式列表 = [
                            r'(?:\\xdb){13}\s{3}LABEL(?:\.){5}:\s(.+?)\s+\\xdb\\xdb\\n\s{9}(?:\\xdb){13}\s{3}CAT.NR\.+:',
                            r'(?:\\xdb){13}\s{3}LABEL(?:\.){5}:\s(.+?)\s+\\xdb\\xdb\\n\s{9}(?:\\xdb){13}\s{3}TRACKS\.+:',
                            r'(?:\\xdb){13}\s{3}LABEL.....:\s(.+?)\s+\\n\s{9}(?:\\xdb){13}\s{3}CAT.NR',
                            r'(?:\\xdb){2}\s(?:\\xdb){5}\\xdf\s{3}Label(?:\\xc4){4}:\s(.+?)\\n\s{9}\\xb1\\xdb\\xdb',
                            r'--------\\n\\n\s{5}Label:\s(.+?)\\n\s{5}Cat#',
                            r':::::\|\s\slabel....:\s(.+?)\s+\|::::\\xb7\\n',
                            r':::\s\sLABEL\s....:\s(.+?)\s+:::\\n\s{5}:::\s\sGENRE',
                            r':\\n\s{4}:\s{3}Label(?:\.){7}:\s(.+?)\s+:\\n\s{4}:\s{3}Genre(?:\.){7}:',
                            r':\\n\s{4}:\s{4}Label.......:\s(.+?)\s+..\s.:..\s.\\n\s{9}Genre.......:',
                            r'LABEL------=\]-:\s(.+?)\\nCAT.\sNR',
                            r'LABEL...\[\s(.+)\s+ENCODER',
                            r'LABEL\s+:\s(.+?)\\xdb\s\\xdb\\xdb\\n',
                            r'Label\\xc4\\xc4\\xc4\\xc4:\s(.+?)\\n',
                            r'Label\s+-\s(.+)\s+Size',
                            r'Label\s{4}\|\s(.+?)\\n\s{12}Catalog',
                            r'\.\\n\s\.\s{3}Label\s{8}:\s(.+?)\s+\.\\n\s\.\s{3}Year\s{9}:\s',
                            r'\\nLABEL(?:\.){3}:\s(.+?)\\nCAT-NR(?:\.){2}:',
                            r'\\nLABEL(?:\.){6}:\s(.+?)\\nGENRE(?:\.){6}:',
                            r'\\nLABEL......:\s(.+?)\\nCAT........:',
                            r'\\nLABEL......:\s(.+?)\\nTRACKS.....:',
                            r'\\nLABEL...\s(.+?)\s+\\nCATNUM',
                            r'\\nLABEL:\s(.+?)\\nGENRE:\s',
                            r'\\nLABEL\s{6}:\s(.+?)\\nGENRE\s{6}:',
                            r'\\nLABEL\s{7}:\s(.+?)\\nRiPDATE',
                            r'\\nLABEL\s{7}:\s(.+?)\\nRiPDATE\s{5}:',
                            r'\\nL\sA\sB\sE\sL...\s(.+?)\\nC\sA\sT\sA\sL\sO\sG',
                            r'\\nLabel(?:\.){7}:\s(.+?)\\nGenre(?:\.){7}:',
                            r'\\nLabel(?:\.){9}::\s(.+?)\\nCatalognr(?:\.){5}::',
                            r'\\nLabel:\s(.+?)\\n\\nTracks:',
                            r'\\n\.:LABEL....\s(.+?)\\n\.:GENRE....',
                            r'\\n\\nLabel\s{7}:\s(.+?)\\nCatalog\s{5}:',
                            r'\\n\\nLabel\s{7}\.\s(.+?)\\nCat Number\s\s\.',
                            r'\\n\\n\\n\s{8}Label.........................:\s(.+?)\\n\s{8}Genre',
                            r'\\n\\n\s{2}Label....:\s{3}(.+?)\\n\s{2}Genre....:',
                            r'\\n\\n\s{5}Label\s{3}\|\s(.+?)\s+Store\s{3}\|',
                            r'\\n\\tLabel\s.......\s:\s(.+?)\\n\\tCatnr\s.......\s:',
                            r'\\n\s:Label:\.{10}:\s(.+?)\\n\s:Catalog:\.{8}:\s',
                            r'\\n\s:Label:__________:\s(.+?)\\n\s:Catalog:________:',
                            r'\\n\sLABEL\s{3}:\s(.+?)\\n\sURL\s{5}:',
                            r'\\n\sLabel......:\s(.+?)\s+\\n\sCatalogNr..:',
                            r'\\n\sLabel\s{4}:\s(.+?)\\n\sSource\s{3}:',
                            r'\\n\sRecord\sLabel\s......::\s(.+?)\\n\sCatalogue\sNumber\s..::',
                            r'\\n\s\sLABEL..........:\s(.+?)\\n\\n\s\sSTORE.DATE',
                            r'\\n\s\sLabel....:\s(.+?)\\n\s\sCatalognr:',
                            r'\\n\s{12}LABEL:........(.+?)\s+GRABBER:',
                            r'\\n\s{12}lABEL\.\.\.\.:\s(.+?)\\n\s{12}lANGUAGE\.:',
                            r'\\n\s{17}\[:\sLABEL......\s:\]\s(.+?)\\n\s{17}\[:\sCAT',
                            r'\\n\s{17}\[:\sLABEL......\s:\]\s(.+?)\\n\s{17}\[:\sTRACKS.....\s:\]',
                            r'\\n\s{20}LABEL\.:\s(.+?)\\n\s{20}CAT\.\.#:',
                            r'\\n\s{21}LABEL(?:\.){6}:\s(.+?)\\n\s{21}GENRE(?:\.){6}:',
                            r'\\n\s{21}LABEL......:\s(.+?)\\n\s{21}RETAIL DATE:',
                            r'\\n\s{2}Label\s{3}:\s(.+?)\\n\s{2}Titel\s{3}:',
                            r'\\n\s{3}Label.....:\s(.+?)\\n\s{3}Genre.....:',
                            r'\\n\s{4}Label(?:\.){5}:\s(.+?)\\n\s{4}Catalog(?:\.){3}:',
                            r'\\n\s{4}Label.....:\s(.+?)\\n\s{4}Catalog...:',
                            r'\\n\s{4}Label\s{8}:\s(.+?)\\n\s{4}Genre',
                            r'\\n\s{5}Label\.{7}:\s(.+?)\\n\s{5}Cat.No\.{6}:\s',
                            r'\\n\s{5}Label\s(?:\.){10}:\s(.+?)\\n\s{5}Language\s(?:\.){7}:',
                            r'\\n\s{5}Label\s{7}:\s(.+?)\\n',
                            r'\\n\s{5}\\xb0\s{2}\\xdb\\xdd\s{3}Label\.:\s(.+?)\s+\\xde\\xdb\s{2}\\xb0\\n\s{6}\\xb0\s\\xde\\xdb\s{3}Genre\.:',
                            r'\\n\s{5}\\xb3\s{3}Label\s{7}:\s(.+?)\\n\s{5}\\xb3\s{3}Catalognr\s{3}:',
                            r'\\n\s{5}\\xdb\\xdb\s{4}\\xdb\\xdb\\xdb\s{3}\\xdb\\xdb\\xdb\s{5}\\xdb\\xdb\s{4}LABEL\.{10}:\s(.+?)\s+\\xdb\\xdb\s{4}\\xdb\\xdb\\xdb\s{4}\\xdb\\xdb\s{4}\\xdb\\xdb\\xdb\\n',
                            r'\\n\s{5}label\s{4}:\s(.+?)\\n\s{5}cat\s{6}\.',
                            r'\\n\s{6}Label\s{4}\|\s(.+?)\\n\s{6}Catalog\s\s\|\s',
                            r'\\n\s{6}Label\s{7}:\s(.+?)\\n\s{6}Genre\s{7}:',
                            r'\\n\s{6}Label\s{7}:\s{2}(.+?)\\n\s{6}Titel',
                            r'\\n\s{6}\|Label\s{7}:\s(.+?)\\n',
                            r'\\n\s{6}label....:\s(.+?)\s+:::::::::\\n',
                            r'\\n\s{7}Company.:\s(.+?)\s+Length..:',
                            r'\\n\s{7}Company.:\s(.+?)\s+Size....:',
                            r'\\n\s{7}LABEL\s(.+?)\\n\s{7}CAT.NR',
                            r'\\n\s{7}LABEL\s{6}:\s(.+?)\\n\s{7}CAT.NUMBER',
                            r'\\n\s{7}label......:\s(.+?)\\n\s{7}genre......:',
                            r'\\n\s{8}LABEL:\s(.+?)\\n\s{8}GENRE:\s',
                            r'\\n\s{8}Label:\s(.+?)\s+Cat.no:',
                            r'\\n\s{8}Label\s{3}\\xb3\s(.+?)\\n\s{8}Genre\s{3}\\xb3\s',
                            r'\\n\s{8}\*\slabel\s+.........:\s(.+?)\\n\s{8}\*\scatalog',
                            r'\\n\s{9}\\xdb\\xdb\\xdb\s{3}LABEL\.\.\.\.\.\[\s(.+?)\s+\\xdb\\xdb\\xdb\\n',
                            r'\\xb0\\xb0\\n\s{11}lABEL:\s(.+?)\\n\s{12}cATNR:',
                            r'\\xb0\\xff\\xdb\\xdb\\xdd\\xff\sLabel....:\s(.+?)\s+\\xff\\xff\\xdb\\xdb\\xdd\\xff',
                            r'\\xb2\\xdb\\xdb\\xdb\\n\\xdb\\xdb\\xdb\\xb2\s\sLabel\s{7}:\s(.+?)\s+\\xb0*\s+\\xb2\\xdb\\xdb\\xdb\\n\\xdb\\xdb\\xdb\\xb2\s\sCat.\sNumber\s:',
                            r'\\xb2\\xdb\\xdb\\xdb\\xdb\\xdb\\xdb\\xdb\\xb2\\xdf\\xdf\\xdb\\xdb\s{3}Label\s........\s(.+?)\s*\\xdb\\xdb\\xdf\\xdf\\xb2\\xdb\\xdb\\xdb\\xdb\\xdb\\xdb\\xdb\\xb2',
                            r'\\xb2\\xdb\\xdb\\xdb\s{10}LABEL....................:\s(.+?)\s+\\xdb\\xdb\\xdb\\xb2',
                            r'\\xb2\sLABEL........\[\s(.+?)\s+\\xb2',
                            r'\\xb3\\n\s\\xb3\sLabel.....:\s(.+?)\s+\\xb3\\n\s\\xb3\sQuality...:',
                            r'\\xb3\\n\s{5}\\xb3\s{3}Label\s{7}:\s(.+?)\s+\\xb3\\n\s{5}\\xb3\s{3}Catalognr\s{3}:',
                            r'\\xb3\\xdb\\xdb\\xdb\\xb3\\n\\xb3\\xdb\\xdb\\xdb\\xb3\s{17}LABEL\s\\x07\s(.+?)\s+\\xb3\\xdb\\xdb\\xdb\\xb3\\n\\xb3\\xdb\\xdb\\xdb\\xb3\s{20}NR\s\\x07',
                            r'\\xb3\\xdd\\xde\\xb3LABEL..........:\s(.+?)\s+\\xd5\\xb8\\xd5\\xd5\\xb8\\xdb\\n',
                            r'\\xb3\s{6}Label...........:\s(.+?)\s+\\xb3\\n\s{8}\\xb3\s{6}Rls Date........',
                            r'\\xba\\n\s\s\\xba\s{53}\\xba\\n\s\s\\xba\s\\xfe\s.label............:\s(.+?)\s+\\xba\s\\n\s\s\\xba\s\\xfe\s.cat.nr...........:',
                            r'\\xba\\n\s\s\\xba\s{53}\\xba\\n\s\s\\xba\s\\xfe\s\.label............:\s(.+?)\s+\\xba\\n',
                            r'\\xba\s{5}LABEL:\s(.+?)\s+\\xba\\n\s\\xba\s{5}SIZE:',
                            r'\\xdb\\n\\xdb\sLabel:\s(.+?)\s+\\xdb\\n\\xdb\sScene:',
                            r'\\xdb\\n\\xdb\s{21}Label:\s(.+?)\s+\\xdb\\n(?:\\xb1){8}\\xdd\s{10}Rip Date:',
                            r'\\xdb\\n\\xdb\s{7}Company.:\s(.+?)\s+Length..:',
                            r'\\xdb\\n\s\s\\xdb\sLabel______:\s(.+?)\s+Size____:',
                            r'\\xdb\\n\s\s\\xdb\s\s\\xb0\s\sLabel\s...\s:\s(.+?)\s+\\xdb\\n\s\s\\xdb\s\s\\xb0\s\sGenre\s...\s:',
                            r'\\xdb\\n\s{5}\\xdb\s{5}LABEL(?:\.){4}>\s(.+?)\s+\\xdb\\n\s{5}\\xdb\s{5}GENRE(?:\.){4}>',
                            r'\\xdb\\n\s{5}\\xdb\s{5}LABEL....>\s(.+?)\s+\\xdb\\n\s{5}\\xdb\s{5}GENRE....',
                            r'\\xdb\\xb0\\xdb\s\\xdb\s{11}Label\s:\s(.+?)\s+\\xdb\s\\xdb\s\\xdb\\n',
                            r'\\xdb\\xdb\\n\\xdb\\xdb\s\sLabel:\s(.+?)\s+\\xdb\\xdb\\n\\xdb\\xdb\s\sScene Date:',
                            r'\\xdb\\xdb\\n\\xdb\\xdb\s{2}Label:\s{7}(.+?)\s+\\xdb\\xdb\\n\\xdb\\xdb',
                            r'\\xdb\\xdb\\xb1\\n\s{5}Label\s{8}\[(.+?)\s+\]\sGenre',
                            r'\\xdb\\xdb\\xb2\s{8}Label\s:\s(.+?)\s+\\xdb\\xdb\\xb2\\n\s{6}\\xdb\\xdb\\xb2\s{9}Year\s:',
                            r'\\xdb\\xdb\\xdb\\n\\xdb\\xdb\s{3}Label\s.......:\s(.+?)\s+\\xdb\\xdb\\xdb\\n\\xdb\\xdb\s{3}Cat.\sNumber\s.:',
                            r'\\xdb\\xdb\\xdb\\xb2\\n\s{4}\\xb2\\xdb\\xdb\\xdb\s{10}LABEL....................:\s(.+?)\s+\\xdb\\xdb\\xdb\\xb2\\n\s{4}\\xb2\\xdb\\xdb\\xdb\s{10}CATALOGUE',
                            r'\\xdb\\xdb\\xdb\\xb2\\n\s{4}\\xb2\\xdb\\xdb\\xdb\s{5}LABEL.............:\s(.+?)\s+\\xdb\\xdb\\xdb\\xb2\\n\s{4}\\xb2\\xdb\\xdb\\xdb\s{5}CATALOGUE.........:',
                            r'\\xdb\\xdb\\xdb\s\\xdb\\xdb\\xdb\\xdb\s\sLabel\s..:\s(.+?)\s+Tagger',
                            r'\\xdb\\xdb\s\\xddLabel___\\xb3\s(.+?)\s+\\xdb\\xdb\\xb2',
                            r'\\xdb\s(?:\\xb1){3}\s\\xb3\\xdd\\xde\\xb3LABEL..........:\s(.+?)\s+\\xd5\\xb8\\xd5\\xd5\\xb8\\xdb\\n',
                            r'\\xdb\s\\xdb\\xdb\\n\s{3}\\xdb\s{4}LABEL\s{7}:\s(.+?)\s+\\xdb\s\\xdb\\xdb\\n\s{3}\\xdb\s{4}RLSDATE\s{5}:',
                            r'\\xdb\s\\xdb\\xdb\\n\s{3}\\xdb\s{4}LABEL\s{7}:\s(.+?)\s+\\xdb\s\\xdb\\xdb\\n\s{3}\\xdb\s{4}TiTEL',
                            r'\\xdc\\xb2\\xdf\\n\\xb1\\xb1\\xdb\\xb0\s\sLABEL(?:\.){3}:\s(.+?)\s+\\xb1\\xdb\\xb0\s\\xdc\\xb2\\xdf\\xb0\\xdd\\n\\xb0\\xdb\\xdb\\xb0\s\sRIP DATE:',
                            r'\\xdc\s{5}LABEL:\s(.+?)\s+\\xdc\s{7}\\xba\\n\\xba\s{5}\\xdc\\xb2\s{62}\\xb2\\xdc\s{5}\\xba\\n\\xba\s{3}\\xdc\\xdb\\xdb\\xb0\s{6}CATALOG:',
                            r'\\xfe\\xb0\\xfe\s{5}LABEL\s\\xfe\s(.+?)\s+\\xfe\\xb0\\xfe\\n',
                            r'\]\\xba\\n\s\s\\xba\sLABEL.........\[\s(.+?)\s+\]\\xba\\n',
                            r'\]\\xba\\n\s{2}\\xba\sLABEL.........\[\s(.+?)\s+\]\\xba\\n\s{2}\\xcc\\xfa(?:\\xc4){5}',
                            r'\s{5}Label\s{4}:\s(.+?)\s+\|\\n\|\s{5}Retail\s{3}:',
                            r'\|\\n\s\s\|\s\sLabel\s{13}<>\s\s(.+?)\s+\|\\n\s\s\|\s\sCat\sNo\.\s{11}<>\s\s',
                            r'\|\\n\s\s\|\s\sLabel_______________\s\s(.+?)\s+\|\\n\s\s|\s\sGenre_______________',
                            r'\|\\n\s\|\s{3}Label\s{4}:\s(.+?)\s+\|\\n\s\|\s{69}\|\\n\s\|',
                            r'\|\\n\|\s{5}Label\s{4}:\s(.+?)\s+\|\\n\|\s{5}Retail\s{3}:',
                            r'label:\s(.+?)\\n\s{6}catalog#:',
                            r'label\s>\s(.+?)\\n',

                        ]

                        for i, 正则表达式 in enumerate(正则表达式列表, start=1):
                            if re.findall(正则表达式, 检测文本内容):
                                厂牌 = re.findall(正则表达式, 检测文本内容)[0]
                                print('命中' + str(i))
                                break

                    if 厂牌 and 厂牌.strip() != 'N/A':
                        非法字符 = r"[\/\\\:\*\?\"\<\>\|]"
                        厂牌 = re.sub(r'\\r', r"", 厂牌)
                        厂牌 = re.sub(r'\\n', r"", 厂牌)
                        厂牌 = re.sub(非法字符, "-", 厂牌)
                        目标目录 = os.path.join(r'B:\2.脚本', 厂牌.strip(), 整理目录列表单个)
                        来源目录 = 根目录
                        print(厂牌.strip())
                        print(来源目录)
                        print(目标目录)
                        shutil.move(来源目录, 目标目录)
                        厂牌 = ''
                        print('++++++++++++++++++++++')
                        break
                    elif 厂牌.strip() == 'N/A':
                        shutil.move(根目录, 没有nfo目录)
                        厂牌 = ''
                        break

    没有nfo列表 = list(set(原始目录列表) - set(有nfo列表))
    if 没有nfo列表:
        for 没有nfo单个目录 in 没有nfo列表:
            shutil.move(没有nfo单个目录, 没有nfo目录)
    return 有nfo列表


if __name__ == "__main__":
    读取nfo(整理目录)
    整理nfo(整理目录)
    读取链接(整理目录)
    整理音乐节(整理目录)
