import os
from openai import OpenAI

client = OpenAI(
    api_key="xxxx",
    base_url="https://api.deepseek.com",
)

# 构造提示词
# prompt = "分析股票[AAL]，查询最新财报信息和近期消息面，返回100字以内摘要总结，摘要应包括公司主营业务，董事会成员，最近财报营收增长，现金流以及估值等情况（请给出具体数字），大行评级以及预测等。"
prompt = "如何让你快速具备联网检索能力"

# ---------- 调用 DeepSeek API ----------
try:
    client = OpenAI(
        api_key="xxxx",
        base_url="https://api.deepseek.com",
    )
    tools = [
        {
            "type": "function",
            "function": {
                "name": "web_search",
                "description": "联网搜索互联网实时信息，用于查询最新新闻、行情、政策、实时数据",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "keyword": {"type": "string", "description": "搜索关键词"}
                    },
                    "required": ["keyword"],
                },
            },
        }
    ]
    response = client.chat.completions.create(
        model="deepseek-v4-pro",  # 使用基础对话模型
        messages=[
            {
                "role": "system",
                "content": "你是一位专业的股票分析师，擅长解读市场信息。",
            },
            {"role": "user", "content": prompt},
        ],
        stream=False,
        reasoning_effort="high",
        extra_body={"thinking": {"type": "enabled"}},
    )

    result = response.choices[0].message.content
    print(f"**分析请求**：{prompt}\n\n---\n\n**AI 回复**：\n{result}")

except Exception as e:
    print(f"❌ API 调用失败：{str(e)}")
