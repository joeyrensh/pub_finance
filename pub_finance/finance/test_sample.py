import requests
from lxml import html

# 发起HTTP请求
url = 'https://sh.lianjia.com/xiaoqu/minhang/pg1/'  # 替换为目标URL
# 添加请求头
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
response = requests.get(url, headers=headers)

# 检查请求是否成功
if response.status_code == 200:
    # 使用lxml解析HTML
    tree = html.fromstring(response.content)
    
    # 查找特定的div
    # 假设我们要查找class为'target-div'的div
    list_contents = tree.xpath('//div[@class="content"]/div[@class="leftContent"]/ul[@class="listContent"]/li[@class="clear xiaoquListItem"]')
    dict = {}
    list = []
    if list_contents:
            for list_content in list_contents:

                # 获取data-id属性的值
                data_id = list_content.get('data-id')
                if data_id:
                    print("data-id属性的值:", data_id)
                else:
                    print("未找到data-id属性")
                
                # 查找<li>标签下的<img>标签，并获取alt属性的值
                img_tag = list_content.xpath('.//img[@class="lj-lazy"]')
                if img_tag:
                    alt_text = img_tag[0].get('alt')
                    if alt_text:
                        print("alt属性的值:", alt_text)
                    else:
                        print("未找到alt属性")
                else:
                    print("未找到目标<img>标签")
                dict = {
                    'data_id': data_id,
                    'al_text': alt_text
                }
                list.append(dict)
    else:
        print("未找到目标<ul>标签")
    print(list)
else:
    print("请求失败，状态码:", response.status_code)