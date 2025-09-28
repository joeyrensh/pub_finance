import re


def extract_arrow_num(s):
    # 去除HTML标签
    clean_text = re.sub("<[^<]+?>", "", s)

    # 提取向上箭头后的数字（新格式：↑7/4）
    arrow_num = re.search(r"↑(\d+)", clean_text)
    arrow_num = int(arrow_num.group(1)) if arrow_num else None

    # 提取斜杠后的当前排名数字（新格式：↑7/4）
    bracket_num = re.search(r"/(\d+)", clean_text)
    bracket_num = int(bracket_num.group(1)) if bracket_num else None

    return arrow_num, bracket_num


str = '<p>资本市场<span style="color:red;"><b>↑</b>1</span>/29</p>'

s1, s2 = extract_arrow_num(str)
print(s1, s2)
