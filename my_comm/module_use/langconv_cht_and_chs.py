from third_party.langconv.langconv import Converter

def cht_to_chs(word):
    line = Converter('zh-hans').convert(word)
    line.encode('utf-8')
    return line

a='轉換繁體到簡體'
b=cht_to_chs(a)
print(b)