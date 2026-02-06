from collections import Counter
import pandas as pd

industry_counter = Counter()

df = pd.DataFrame(
    {"a1": ["a", "a", "a", "a", "a", "b", "b", "b", "b", "c", "c", "c", "d", "d", "d"]}
)


for _, row in df.iterrows():
    items = row["a1"]
    if not items:
        continue
    for item in items:
        if item:
            industry_counter[str(item).strip()] += 1

top3_industries = {industry for industry, _ in industry_counter.most_common(3)}
print(top3_industries)
