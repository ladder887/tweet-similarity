import re
import pandas as pd
from collections import Counter
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import os

def cos_similarity(set1, set2):
    try:
        vectorizer = TfidfVectorizer(stop_words='english')
        tfidf_matrix = vectorizer.fit_transform([set1, set2])
        cosine_sim = cosine_similarity(tfidf_matrix)
        return cosine_sim[0][1]
    except:
        return 'err'

def make_frequency(words):
    return Counter(words)

def jac_similarity(set1, set2):
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    similarity = intersection / union
    return similarity

def hashtag_similarity(set1, set2):
    if not pd.notna(set1) or not pd.notna(set2):
        return 0
    set1 = set(set1.strip('[ ]').split(', '))
    set2 = set(set2.strip('[ ]').split(', '))
    return jac_similarity(set1, set2)

def content_similarity(set1, set2):
    if not set1 or not set2:
        return 0
    return cos_similarity(set1, set2)

def content_processing(data):
    if not pd.notna(data):
        return ""
    url_del = r'http[s]?://[^\s]+'
    hashtag_del = r'#\w+'
    data = re.sub(url_del, '', data)
    data = re.sub(hashtag_del, '', data)
    data = data.strip()
    return data

def save_csv():
    if os.path.isfile(result_csv_file_path):
        df_1.to_csv(result_csv_file_path, mode='a', header=False, index=False, encoding="utf-8-sig")
    else:
        df_1.to_csv(result_csv_file_path, index=False, encoding="utf-8-sig")

def url_similarity(set1, set2):
    if set1 == set2:
        return 1
    return 0

# csv 파일 로딩
csv_file_path = 'C:\\Users\\Cysec\\Desktop\\knowledge_graph\\sampleRetweeted.csv'
result_csv_file_path = 'C:\\Users\\Cysec\\Desktop\\knowledge_graph\\sampleSim.csv'

df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
df_1 = pd.DataFrame()
count = 0

for standard in df.itertuples():
    for comparison_index in range(standard.Index+1, len(df)):
        row = {}
        comparison = df.iloc[comparison_index]

        if standard.AccountID == comparison.AccountID:
            continue

        row['StandardTweetID'] = standard.TweetID
        row['ComparisonTweetID'] = standard.TweetID
        row['StandardAccountID'] = standard.AccountID
        row['ComparisonAccountID'] = comparison.AccountID
        row['StandardTweetContent'] = content_processing(standard.TweetContent)
        row['ComparisonTweetContent'] = content_processing(comparison.TweetContent)
        row['TweetHashtagSimilarity'] = hashtag_similarity(standard.TweetContentHashtag, comparison.TweetContentHashtag)
        row['TweetContentSimilarity'] = content_similarity(row['StandardTweetContent'], row['ComparisonTweetContent'])
        row['AccountHashtagSimilarity'] = hashtag_similarity(standard.AccountDescriptionHashtag, comparison.AccountDescriptionHashtag)
        row['AccountContentSimilarity'] = content_similarity(standard.AccountDescription, comparison.AccountDescription)
        row['TweetUrl'] = url_similarity(standard.TweetContentURL, comparison.TweetContentURL)
        row['AccountUrl'] = url_similarity(standard.AccountDescriptionURL, comparison.AccountDescriptionURL)

        df_1 = df_1._append(row, ignore_index=True)
        count += 1
        #print(count, row)
    print(count)
save_csv()