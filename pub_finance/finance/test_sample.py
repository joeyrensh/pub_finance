import nltk

nltk.download("punkt")
from nltk.tokenize import word_tokenize, sent_tokenize

text = "这是一个复杂的句子，我们希望将其简化为几个短语。"
words = word_tokenize(text)
print(words)  # 输出分词结果
