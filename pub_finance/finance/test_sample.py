import os
from openai import OpenAI

client = OpenAI(
    # 若没有配置环境变量，请用百炼API Key将下行替换为：api_key="sk-xxx"
    api_key=os.getenv("API_KEY"),
    base_url="https://ws-uaeaan6mql1ieioa.cn-beijing.maas.aliyuncs.com/compatible-mode/v1",
)

response = client.responses.create(
    model="qwen3.7-plus",
    input="你是一名专业的股票分析师，请查询美股'NOW'最新财报信息，请分析公司财务状况(包括营收增长，现金流等)，利好消息，管理层动作，100字以内。",
    tools=[
        {"type": "web_search"},
        {"type": "code_interpreter"},
        {"type": "web_extractor"},
    ],
)

# 获取模型回复
print(response.output_text)
