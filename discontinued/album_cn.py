def 规范化文件夹(整理目录):
    艺人目录列表 = os.listdir(整理目录)  # 艺人目录
    for 艺人目录名 in 艺人目录列表:
        文件总列表 = os.listdir(os.path.join(整理目录, 艺人目录名))
        for 文件 in 文件总列表:
            if os.path.isfile(os.path.join(整理目录, 艺人目录名, 文件)):
                os.makedirs(os.path.join(整理目录, 艺人目录名, '@@@@@@'), exist_ok=True)
                shutil.move(os.path.join(整理目录, 艺人目录名, 文件), os.path.join(整理目录, 艺人目录名, '@@@@@@'))
        文件夹列表 = os.listdir(os.path.join(整理目录, 艺人目录名))

        def 艺名检测(*检测值表):
            for 检测值 in 检测值表:
                检测值 = 繁体转简体(检测值)
                if re.findall(艺人目录名, 检测值):
                    检测值 = 艺人目录名
                    break
                elif re.findall(r'群星', 检测值):
                    检测值 = '群星'
                    break
            return (检测值)

        for 文件夹名 in 文件夹列表:
            去除重复标记文件夹名 = 文件夹名.upper()
            替换列表 = {'【': '[', '】': ']', '（': '(', '）': ')', '&AMP;': ' ', '–': '-', '—': "-",
                        'Ⅰ': '1', 'Ⅱ': '2', 'Ⅲ': '3', 'Ⅳ': '4', 'Ⅴ': '5', 'Ⅵ': '6', 'Ⅶ': '7', 'Ⅷ': '8', 'Ⅸ': '9', 'Ⅹ': '10', 'Ⅺ': '11', 'Ⅻ': '12',
                        '①': '1', '②': '2', '③': '3', '④': '4', '⑤': '5', '⑥': '6', '⑦': '7', '⑧': '8', '⑨': '9', '⑩': '10',
                        }
            for k, v in 替换列表.items():
                去除重复标记文件夹名 = 去除重复标记文件夹名.replace(k, v)
            去除重复标记文件夹名 = 繁体转简体(去除重复标记文件夹名)
            去除重复标记文件夹名 = re.sub(r"\((原始文件|\d)\)", "", 去除重复标记文件夹名).strip()  # 崔(原始文件) 崔(1)
            去除重复标记文件夹名 = re.sub(r"_\d$", "", 去除重复标记文件夹名).strip()  # 崔_2
            去除重复标记文件夹名 = re.sub(r"\s\s+", " ", 去除重复标记文件夹名).strip()  # 多个空格
            去除重复标记文件夹名 = re.sub(r"^(320K_|DTS-|CD\d+)", "", 去除重复标记文件夹名).strip()  # 320k_ DTS- CD7
            # _副本 已修复 无水印封面 封面 正版CD低速原抓 正版CD原抓 正版原抓 无水印大图
            去除重复标记文件夹名 = re.sub(r"(_副本|已修复|无水印封面|封面|正版CD低速原抓|正版CD原抓|正版原抓|无水印大图)", "", 去除重复标记文件夹名).strip()
            去除重复标记文件夹名 = re.sub(r"(\[|[(])*(UPDTS.+分轨|320K(\s|\.|_)MP3|MP3(\s|\.|_)320K)(\]|[)])*", " ", 去除重复标记文件夹名).strip()  # UPDTS-分轨 320K(\s|.|_)MP3 MP3(\s|.|_)320K
            去除重复标记文件夹名 = re.sub(r"(\[|[(])*MP3版共.+首(\]|[)])*", " ", 去除重复标记文件夹名).strip()  # MP3版共14首
            # DSDH DSD DXD BSCD HQCD HQII HQⅡ XRCD2 光盘映像NRG LPCDR45 LPCD45 LP-CD45 K2HD 6N纯银SQCD 320K AQCD AMCD
            去除重复标记文件夹名 = re.sub(r"(\[|[(])*(DSDH|DSD|DXD|BSCD|HQCD|LPCD|HQ|AMCD|XRCD2|光盘映像NRG|LPCDR45|LPCD45|LP-CD45|K2HD|6N纯银SQCD|320K|AQCD)(\]|[)])*", " ", 去除重复标记文件夹名).strip()
            去除重复标记文件夹名 = re.sub(r"(\[|[(])*DTS-(ES61|WAV|WAV分轨|ES)(\]|[)])*", " ", 去除重复标记文件夹名).strip()  # DTS-ES61 DTS-WAV DTS-ES DTS-WAV分轨
            去除重复标记文件夹名 = re.sub(r"(\[|[(])*(TP|LP)-(APE|WAV)(\]|[)])*", " ", 去除重复标记文件夹名).strip()  # TP-APE LP-APE
            去除重复标记文件夹名 = re.sub(r"[\[({]*(MP3|FLAC|APE|WAV|DTS|DFF|SACD)\s*(分轨|整轨)*(\+*\s*CUE)*[\])}]*", " ", 去除重复标记文件夹名).strip()  # (MP3|FLAC|APE|WAV|DTS|DFF|SACD)(分轨|整轨)*(+CUE)*
            去除重复标记文件夹名 = re.sub(r"专辑$", "", 去除重复标记文件夹名).strip()  # 专辑

            r1 = re.findall(r'^\d{4}-\[\w+](\w+)(?:[-\s](?:[^《]*))*《(.+)》(.*)', 去除重复标记文件夹名)  # 3715-[华语]陈思安-台湾新生代《酒廊情歌(K2HD) 2CD》2015[WAV分轨]CD2
            r11 = re.findall(r'^(\w+)(?:\s|-|\.)(\d{4})(?:-\d{2})*(?:\s|-|\.)([^《]+)', 去除重复标记文件夹名)  # 张智霖(-|.| )1996(-00)(-00)(-|.| )言不由衷[香港]
            r7 = re.findall(r'^(\w+)(\d{4})(?:\d{2}-)*-([^\d][^《]*)(\[*.*]*)', 去除重复标记文件夹名)  # 张智霖1991-粉红色的回忆[引进版][WAV整轨]
            r12 = re.findall(r'^(\w+)(?:\s|\.)*-(?:\s|\.)*\[(.*)](.*)', 去除重复标记文件夹名)  # 萧亚轩(\s|.)-(\s|.)[WOW3] LIVE DVD2
            r10 = re.findall(r'^(\d{4})(?:\s|\.|-)+(\w+)(?:\s|\.|-)+(.*)', 去除重复标记文件夹名)  # 2006(\s|.|-)李克勤(\s|.|-)我着10号 EPISO
            r16 = re.findall(r'^([^《&]+)《(\d{4})\s([^》]+)》(.*)', 去除重复标记文件夹名)  # 孟庭苇《1993 风中有朵雨做的云》[WAV 整轨]
            r4 = re.findall(r'^([^《&]+)《([^》]+)》(.*)', 去除重复标记文件夹名)  # 韩宝仪(^&)《福建巨星》(新加坡版)
            r3 = re.findall(r'^《([^》]+)》(.*)', 去除重复标记文件夹名)  # 《第六辑》(马来亚版)
            r8 = re.findall(r'^([^《]+)《([^》]+)$', 去除重复标记文件夹名)  # 林一峰《Playlist
            r20 = re.findall(r'^(\d{4})\s*-\s*([^《-]+)', 去除重复标记文件夹名)  # 1995-Rain
            r99 = re.findall(r'^([^《》\s&\-]+)(?:\s|\.)*[-－_—：·\s]+(?:\s|\.)*([^《&\-》]+)(.*)', 去除重复标记文件夹名)  # 孟庭苇－孟庭苇经典   或   黄凯芹_挥不去的情感

            if r1:
                艺名 = r1[0][0].strip()
                专辑 = r1[0][1].strip()
                其他 = r1[0][2].strip()
                艺名 = 艺名检测(艺名)
                新文件夹名 = 艺名 + '《' + 专辑 + '》' + 其他
            elif r11:
                艺名 = r11[0][0].strip()
                专辑 = r11[0][2].strip()
                年份 = r11[0][1].strip()
                艺名 = 艺名检测(艺名)
                新文件夹名 = 艺名 + '《' + 专辑 + '》' + 年份
            elif r7:
                艺名 = r7[0][0].strip()
                专辑 = r7[0][2].strip()
                年份 = r7[0][1].strip()
                其他 = r7[0][3].strip()
                艺名 = 艺名检测(艺名)
                新文件夹名 = 艺名 + '《' + 专辑 + '》' + 年份 + ' ' + 其他
            elif r12:
                艺名 = r12[0][0].strip()
                专辑 = r12[0][1].strip()
                其他 = r12[0][2].strip()
                艺名 = 艺名检测(艺名)
                新文件夹名 = 艺名 + '《' + 专辑 + '》' + 其他
            elif r10:
                艺名 = r10[0][1].strip()
                专辑 = r10[0][2].strip()
                年份 = r10[0][0].strip()
                艺名 = 艺名检测(艺名)
                新文件夹名 = 艺名 + '《' + 专辑 + '》' + 年份
            elif r16:
                艺名 = r16[0][0].strip()
                年份 = r16[0][1].strip()
                专辑 = r16[0][2].strip()
                艺名 = 艺名检测(艺名)
                新文件夹名 = 艺名 + '《' + 专辑 + '》' + 年份
            elif r4:
                艺名 = r4[0][0].strip()
                专辑 = r4[0][1].strip()
                其他 = r4[0][2].strip()
                艺名 = 艺名检测(艺名)
                if 艺名 != 艺人目录名 and re.findall(艺人目录名, 专辑):
                    艺名 = '群星'
                新文件夹名 = 艺名 + '《' + 专辑 + '》' + 其他
            elif r3:
                专辑 = r3[0][0].strip()
                其他 = r3[0][1].strip()
                if re.findall(艺人目录名, 专辑):
                    艺名 = '群星'
                else:
                    艺名 = 艺人目录名
                新文件夹名 = 艺名 + '《' + 专辑 + '》' + 其他
            elif r20:
                专辑 = r20[0][1].strip()
                年份 = r20[0][0].strip()
                艺名 = 艺人目录名
                新文件夹名 = 艺名 + '《' + 专辑 + '》' + 年份
            elif r8:
                艺名 = r8[0][0].strip()
                专辑 = r8[0][1].strip()
                艺名 = 艺名检测(艺名)
                新文件夹名 = 艺名 + '《' + 专辑 + '》'
            elif r99:
                艺名 = r99[0][0].strip()
                专辑 = r99[0][1].strip()
                其他 = r99[0][2].strip()
                艺名 = 艺名检测(艺名)
                if 艺名 == 艺人目录名 or 艺名 == '群星':
                    新文件夹名 = 艺名 + '《' + 专辑 + '》' + 其他
                else:
                    新文件夹名 = 去除重复标记文件夹名
            else:
                新文件夹名 = 去除重复标记文件夹名

            原路径 = os.path.join(整理目录, 艺人目录名, 文件夹名)
            新路径 = os.path.join(整理目录, 艺人目录名, 新文件夹名).strip()

            def 检测音频(路径):  # 返回 0=已删除,1=杂文件,2=有音频,3=不变
                删除空文件夹(路径)
                if not os.path.exists(路径):
                    return 0
                else:
                    for 根目录, 目录列表, 文件列表 in os.walk(路径):
                        for 文件 in 文件列表:
                            文件后缀 = os.path.splitext(文件)[1].upper()
                            if 文件后缀 in 音频后缀列表:
                                return 2
                return 1

            if 原路径.upper() != 新路径 and os.path.exists(新路径):
                原路径文件夹状态 = 检测音频(原路径)
                新路径文件夹状态 = 检测音频(新路径)
            else:
                原路径文件夹状态 = 新路径文件夹状态 = 3

            if 新路径文件夹状态 == 1 and 原路径文件夹状态 == 2 or 新路径文件夹状态 == 原路径文件夹状态 == 1:
                多余文件路径列表, 多余文件列表 = 获取所有文件(新路径)
                for m in range(len(多余文件路径列表)):
                    源目录 = 多余文件路径列表[m]
                    终目录 = os.path.join(原路径, 多余文件列表[m])
                    移动目标(源目录, 终目录)
                删除空文件夹(新路径)
                if not os.path.exists(新路径):
                    新路径文件夹状态 = 0
            elif 原路径文件夹状态 == 1 and 新路径文件夹状态 == 2:
                多余文件路径列表, 多余文件列表 = 获取所有文件(原路径)
                for m in range(len(多余文件路径列表)):
                    源目录 = 多余文件路径列表[m]
                    终目录 = os.path.join(新路径, 多余文件列表[m])
                    移动目标(源目录, 终目录)
                删除空文件夹(原路径)
                if not os.path.exists(原路径):
                    原路径文件夹状态 = 0

            if 文件夹名.upper() != 新文件夹名:
                if 新路径文件夹状态 == 0:
                    print(文件夹名 + '  重命名到:  ' + 新文件夹名)
                    移动目标(原路径, 新路径)
                elif 原路径文件夹状态 == 0:
                    print(文件夹名 + '  并入到:  ' + 新文件夹名)
                elif 原路径文件夹状态 == 新路径文件夹状态 == 2:
                    print(文件夹名 + '  重命名到:  ' + 新文件夹名)
                    移动目标(原路径, 新路径)
                elif 原路径文件夹状态 == 新路径文件夹状态 == 3:
                    print(文件夹名 + '  重命名到:  ' + 新文件夹名)
                    移动目标(原路径, 新路径)


def 提取多余文件(整理目录, 图片=0):
    整理目录列表 = os.listdir(整理目录)

    def 按专辑目录处理(专辑目录路径):
        碟数目 = 0
        卷数目 = 0
        for 根目录, 目录列表, 总文件列表 in os.walk(专辑目录路径):
            有音频 = 0
            有图片 = 0

            for 文件 in 总文件列表:
                文件后缀 = os.path.splitext(文件)[1].upper()
                if 文件后缀 in 音频后缀列表:
                    有音频 += 1
                elif 文件后缀 in 图片后缀列表:
                    有图片 += 1
            if 有音频 > 0:
                碟数目 += 1
            if 有图片 > 0:
                卷数目 += 1

        for 根目录, 目录列表, 文件列表 in os.walk(专辑目录路径):
            if len(目录列表) == 0 and len(文件列表) == 0:
                os.removedirs(根目录)
            elif (有音频 > 0 and len(目录列表) == 0) or (有图片 > 0 and len(目录列表) == 0):
                pass
            elif (目录列表 and 碟数目 == 1) or (目录列表 and 图片 == 1 and 卷数目 == 1):
                print('处理目录:' + 根目录)
                for k in 目录列表:
                    多余目录路径 = os.path.join(根目录, k)
                    多余文件路径列表, 多余文件列表 = 获取所有文件(多余目录路径)
                    for m in range(len(多余文件路径列表)):
                        源目录 = 多余文件路径列表[m]
                        终目录 = os.path.join(专辑目录路径, 多余文件列表[m])
                        移动目标(源目录, 终目录)
            elif (目录列表 and 10 > 碟数目 > 1):
                for k in 目录列表:
                    多余目录路径 = os.path.join(根目录, k)
                    多余文件路径列表, 多余文件列表 = 获取所有文件(多余目录路径)
                    for m in range(len(多余文件列表)):
                        if os.path.splitext(多余文件列表[m])[1].upper() in 音频后缀列表:
                            按专辑目录处理(多余目录路径)
                            break
                        else:
                            print('处理目录:' + 根目录)
                            源目录 = 多余文件路径列表[m]
                            终目录 = os.path.join(专辑目录路径, 多余文件列表[m])
                            移动目标(源目录, 终目录)

            elif 碟数目 > 10:
                print('预计出现故障,请检查目录:' + 根目录)

    for 一级目录 in 整理目录列表:
        专辑列表 = os.listdir(os.path.join(整理目录, 一级目录))
        for 专辑目录 in 专辑列表:
            专辑目录路径 = os.path.join(整理目录, 一级目录, 专辑目录)
            按专辑目录处理(专辑目录路径)
            if re.findall(r'群星', 专辑目录):
                所有文件路径列表, 所有文件列表 = 获取所有文件(专辑目录路径)
                if 所有文件路径列表:
                    文本文件列表 = [文本文件 for 文本文件 in 所有文件路径列表 if 文本文件.split('.')[-1] == 'txt']
                    厂牌公司 = []
                    if 文本文件列表:
                        for 文本文件 in 文本文件列表:
                            with open(文本文件, 'rb') as 文件:
                                编码 = chardet.detect(文件.read())['encoding']
                            with open(文本文件, 'r', encoding=编码, errors='ignore') as 文件:
                                文件内容 = 文件.read()
                                厂牌公司.extend(re.findall(r'出版公司[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'发行公司[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'出版发行[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'岀版公司[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'专辑出版[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'专辑发行[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'总经销[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'唱片公司[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'唱片发行[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'唱片制作[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'版权发行[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'出品公司[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'出品发行[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'制作发行[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'制作公司[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'出版[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'出版社[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'出版者[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'出品[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                                厂牌公司.extend(re.findall(r'发行[：|:]\s*(.+)\n', 文件内容, flags=re.IGNORECASE))
                        if 厂牌公司:
                            print(专辑目录 + '  可能的厂牌是: ' + str(set(厂牌公司)))
                        else:
                            print(专辑目录 + '  没有厂牌信息')
