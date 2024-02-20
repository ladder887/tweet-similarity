from pymongo import MongoClient
import pandas as pd
import re

class MongoDBHandler: #몽고 디비 핸들러
    def __init__(self, host='localhost', port=27017, database_name='d3'):
        # MongoDB 서버에 연결
        self.client = MongoClient(f'mongodb://{host}:{port}/')
        # 데이터베이스 선택 (없으면 자동으로 생성됨)
        self.db = self.client[database_name]

    def get_collection(self, collection_name): # 몽고디비에 데이터베이스를 선택하고 선택된 데이터베이스 안에 컬랙션이라는 것을 만들 수 있음 데이터베이스는 서비스 전체, 컬랙션은 유저 목록, 상품 종류 이런거임.
        return self.db[collection_name]
    
    def insert_document(self, collection, standard, AccountID):
        # 기존 데이터 가져오기
        existing_doc = collection.find_one({"AccountID": AccountID})

        if existing_doc: #기존에 데이터가 존재하는지 안하는지 확인
            print("AccountID already exists in the database:", AccountID)

            # 기존 데이터 업데이트
            update_result = collection.update_one(
                {"AccountID": AccountID},
                {
                    "$push": {
                        'TweetData': {
                            'TweetID' : id_data_preprocessing(standard.TweetID),
                            'TweetContent' : standard.TweetContent,
                            'TweetContentPreprocessing' : content_data_processing(standard.TweetContent),
                            'AuthorID' : id_data_preprocessing(standard.AuthorID),
                            'OriginalTweetID' : id_data_preprocessing(standard.OriginalTweetID),
                            'TweetCreated' : standard.TweetCreated,
                            'TweetEntities' : nan_data_preprocessing(standard.TweetEntities),
                            'TweetContentHashtag' : list_data_preprocessing(standard.TweetContentHashtag),
                            'TweetContentURL' : list_data_preprocessing(standard.TweetContentURL),
                            'TweetMention' : list_data_preprocessing(standard.TweetMention),
                            'TweetMedia' : nan_data_preprocessing(standard.TweetMedia),
                            'TweetMediaURL' : list_data_preprocessing(standard.TweetMediaURL),
                            'OriginalTweetAuthorID' : id_data_preprocessing(standard.OriginalTweetAuthorID),
                            'TweetLanguage' : standard.TweetLanguage,
                            'TweetRetweetCount' : standard.TweetRetweetCount,
                            'TweetReplyCount' : standard.TweetReplyCount,
                            'TweetLikeCount' : standard.TweetLikeCount,
                            'TweetQuoteCount' : standard.TweetQuoteCount,
                            'TweetBookmarkCount' : standard.TweetBookmarkCount,
                            'TweetImpressionCount' : standard.TweetImpressionCount,
                            'TweetReferencedTweets' : nan_data_preprocessing(standard.TweetReferencedTweets),
                            'TweetSource' : nan_data_preprocessing(standard.TweetSource),
                            'TweetPlace' : nan_data_preprocessing(standard.TweetPlace),
                            'Include' : nan_data_preprocessing(standard.Include),
                            'TweetContextAnnotations' : standard.TweetContextAnnotations
                        }
                    }
                }
            )

            if update_result.modified_count > 0:
                print("데이터 업데이트 성공.")
            else:
                print("데이터 업데이트 실패.")

        else:
            # 기존 데이터가 없을 경우, 새로운 데이터 삽입
            data = {
                'AccountID' : AccountID,
                'AccountNickname' : standard.AccountNickname,
                'AccountName' : standard.AccountName,
                'AccountCreated' : standard.AccountCreated,
                'AccountDescription' : nan_data_preprocessing(standard.AccountDescription),
                'AccountDescriptionPreprocessing' : content_data_processing(standard.AccountDescription),
                'AccountEntities' : nan_data_preprocessing(standard.AccountEntities),
                'AccountDescriptionHashtag' : list_data_preprocessing(standard.AccountDescriptionHashtag),
                'AccountDescriptionURL' : list_data_preprocessing(standard.AccountDescriptionURL),
                'AccountDescriptionMention' : list_data_preprocessing(standard.AccountDescriptionMention),
                'AccountLocation' : nan_data_preprocessing(standard.AccountLocation),
                'AccountProfileImageURL' : nan_data_preprocessing(standard.AccountProfileImageURL),
                'AccountFollowersCount' : standard.AccountFollowersCount,
                'AccountFollowingCount' : standard.AccountFollowingCount,
                'AccountTweetCount' : standard.AccountTweetCount,
                'AccountListedCount' : standard.AccountListedCount,
                'AccountLikeCount' : standard.AccountLikeCount,
                'AccountURL' : nan_data_preprocessing(standard.AccountURL),
                'AccountVerified' : standard.AccountVerified,
                'TweetData' : [{
                    'TweetID' : id_data_preprocessing(standard.TweetID),
                    'TweetContent' : standard.TweetContent,
                    'TweetContentPreprocessing' : content_data_processing(standard.TweetContent),
                    'AuthorID' : id_data_preprocessing(standard.AuthorID),
                    'OriginalTweetID' : id_data_preprocessing(standard.OriginalTweetID),
                    'TweetCreated' : standard.TweetCreated,
                    'TweetEntities' : nan_data_preprocessing(standard.TweetEntities),
                    'TweetContentHashtag' : list_data_preprocessing(standard.TweetContentHashtag),
                    'TweetContentURL' : list_data_preprocessing(standard.TweetContentURL),
                    'TweetMention' : list_data_preprocessing(standard.TweetMention),
                    'TweetMedia' : nan_data_preprocessing(standard.TweetMedia),
                    'TweetMediaURL' : list_data_preprocessing(standard.TweetMediaURL),
                    'OriginalTweetAuthorID' : id_data_preprocessing(standard.OriginalTweetAuthorID),
                    'TweetLanguage' : standard.TweetLanguage,
                    'TweetRetweetCount' : standard.TweetRetweetCount,
                    'TweetReplyCount' : standard.TweetReplyCount,
                    'TweetLikeCount' : standard.TweetLikeCount,
                    'TweetQuoteCount' : standard.TweetQuoteCount,
                    'TweetBookmarkCount' : standard.TweetBookmarkCount,
                    'TweetImpressionCount' : standard.TweetImpressionCount,
                    'TweetReferencedTweets' : nan_data_preprocessing(standard.TweetReferencedTweets),
                    'TweetSource' : nan_data_preprocessing(standard.TweetSource),
                    'TweetPlace' : nan_data_preprocessing(standard.TweetPlace),
                    'Include' : nan_data_preprocessing(standard.Include),
                    'TweetContextAnnotations' : standard.TweetContextAnnotations
                }]
            }

            result = collection.insert_one(data)

            if result.inserted_id:
                print("데이터 생성 완료")
            else:
                print("데이터 생성 실패")

#id형식 데이터 전처리
def id_data_preprocessing(data):
    if not pd.notna(data):
        return ''
    return data.strip('[ ]')

#list형식 데이터 전처리
def list_data_preprocessing(data):
    if not pd.notna(data):
        return []
    return data.strip("'[ ]'").split("', '")

#NaN값 전처리
def nan_data_preprocessing(data):
    if not pd.notna(data):
        return "None"
    return data

#트윗내용 사용자 소개글 전처리
def content_data_processing(data):
    if not pd.notna(data):
        return "None"
    url_del = r'http[s]?://[^\s]+'
    hashtag_del = r'#\w+'
    data = re.sub(url_del, '', data)
    data = re.sub(hashtag_del, '', data)
    data = data.strip()
    if not data:
        return "None"
    return data

if __name__ == "__main__":

    #데이터 위치
    file_path = 'C:\\Users\\Cysec\\Desktop\\knowledge_graph\\data_retweeted_processing\\'
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

    mongo_handler = MongoDBHandler()
    my_collection = mongo_handler.get_collection('tweet')
    count = 0
    my_collection.create_index([('AccountID')], unique = True)

    for csv_name in csv_list:
        csv_file_path = file_path + csv_name
        df = pd.read_csv(csv_file_path, encoding='utf-8-sig')

        for standard in df.itertuples():

            temp = my_collection.find_one({'TweetData': {'$elemMatch' : {'TweetID' :id_data_preprocessing(standard.TweetID)}}})
            if temp:
                print("TweetID already exists in the database: ", standard.TweetID)
                continue
            mongo_handler.insert_document(my_collection, standard, id_data_preprocessing(standard.AccountID))
            count += 1
            print(count)
