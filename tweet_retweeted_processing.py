import pandas as pd
import re

#데이터 위치
file_path = 'C:\\Users\\Cysec\\Desktop\\knowledge_graph\\data\\'
result_file_path = 'C:\\Users\\Cysec\\Desktop\\knowledge_graph\\data_retweeted_processing\\'


csv_list = ["2cb.csv", 
        "acid.csv", 
        "Adderall.csv", 
        "Amphetamine.csv", 
        "benzos.csv", 
        "bud.csv", 
        "carts.csv", 
        "cocaine.csv",
        "coke.csv",
        "DMT.csv",
        "dmtvape.csv",
        "drugs.csv",
        "drugstwt.csv",
        "drugtwt.csv",
        "ecstacy.csv",
        "edibles.csv",
        "Fentanyl.csv",
        "hash.csv",
        "hashish.csv",
        "heroin.csv",
        "hydrocodone.csv",
        "ketamine.csv",
        "kush.csv",
        "lsd.csv",
        "Marijuana.csv",
        "mdma.csv",
        "Mescaline.csv",
        "meth.csv",
        "methamphetamine.csv",
        "mushrooms.csv",
        "nufcfanscoke.csv",
        "pills.csv",
        "plug.csv",
        "psilocybin.csv",
        "psychedelics.csv",
        "search420Life.csv",
        "shrooms.csv",
        "vegasplug.csv",
        "weed.csv",
        "Xanax.csv",
        "1drugtwt.csv",
        "1mdma.csv",
    ]

#리트윗 트윗 데이터 처리함수
def retweeted_processing(csv_file_path, result_csv_file_path):

    df = pd.read_csv(csv_file_path, encoding= 'utf-8')
    for row in df.itertuples():
        #데이터에 nan이 아니고 'retweeted'이 포함되어 있을시
        if not pd.isnull(row.TweetReferencedTweets):
            if 'retweeted' in row.TweetReferencedTweets:
                print(row.TweetReferencedTweets)
                #id= 뒤에 리트윗TweetID 추출
                retweet_ID = re.search(r'id=(\d+)', row.TweetReferencedTweets).group(1)
                #추출한 리트윗TweetID의 데이터 탐색
                retweet_data = df[df['TweetID'] == "[" + str(retweet_ID) + "]"]
                retweet_data = retweet_data.iloc[0]

                #print(retweet_data.TweetContent)
                #print(retweet_data.TweetContentHashtag)
                #print(retweet_data.TweetContentURL)
                
                #리트윗 Tweet 데이터를 현재 Tweet에 덮어쓰기
                df.loc[row.Index, 'TweetContent'] = retweet_data.TweetContent
                df.loc[row.Index, 'TweetContentHashtag'] = retweet_data.TweetContentHashtag
                df.loc[row.Index, 'TweetContentURL'] = retweet_data.TweetContentURL

    #csv 저장
    df.to_csv(result_csv_file_path, index=False, encoding='utf-8-sig')

for csv_name in csv_list:
    csv_file_path = file_path + csv_name
    result_csv_file_path = result_file_path + csv_name
    retweeted_processing(csv_file_path, result_csv_file_path)