from pymongo import MongoClient
import pandas as pd

class MongoDBHandler: #몽고 디비 핸들러
    def __init__(self, host='localhost', port=27017, database_name='d3'):
        # MongoDB 서버에 연결
        self.client = MongoClient(f'mongodb://{host}:{port}/')
        # 데이터베이스 선택 (없으면 자동으로 생성됨)
        self.db = self.client[database_name]

    def get_collection(self, collection_name): # 몽고디비에 데이터베이스를 선택하고 선택된 데이터베이스 안에 컬랙션이라는 것을 만들 수 있음 데이터베이스는 서비스 전체, 컬랙션은 유저 목록, 상품 종류 이런거임.
        return self.db[collection_name]
    
    def insert_document(self, collection, standard):
        print(standard.TweetID)
        # 기존 데이터 가져오기
        existing_doc = collection.find_one({"AccountID": standard.AccountID})

        if existing_doc: #기존에 데이터가 존재하는지 안하는지 확인
            print("AccountID already exists in the database:", standard.AccountID)

            # 기존 데이터 업데이트
            update_result = collection.update_one(
                {"AccountID": standard.AccountID},
                {
                    "$push": {
                        'TweetData': {
                            'TweetID' : standard.TweetID,
                            'TweetContent' : standard.TweetContent,
                            'AuthorID' : standard.AuthorID,
                            'OriginalTweetID' : standard.OriginalTweetID,
                            'TweetCreated' : standard.TweetCreated,
                            'TweetEntities' : standard.TweetEntities,
                            'TweetContentHashtag' : standard.TweetContentHashtag,
                            'TweetContentURL' : standard.TweetContentURL,
                            'TweetMention' : standard.TweetMention,
                            'TweetMedia' : standard.TweetMedia,
                            'TweetMediaURL' : standard.TweetMediaURL,
                            'OriginalTweetAuthorID' : standard.OriginalTweetAuthorID,
                            'TweetLanguage' : standard.TweetLanguage,
                            'TweetRetweetCount' : standard.TweetRetweetCount,
                            'TweetReplyCount' : standard.TweetReplyCount,
                            'TweetLikeCount' : standard.TweetLikeCount,
                            'TweetQuoteCount' : standard.TweetQuoteCount,
                            'TweetBookmarkCount' : standard.TweetBookmarkCount,
                            'TweetImpressionCount' : standard.TweetImpressionCount,
                            'TweetReferencedTweets' : standard.TweetReferencedTweets,
                            'TweetSource' : standard.TweetSource,
                            'TweetPlace' : standard.TweetPlace,
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
                'AccountID' : standard.AccountID,
                'AccountNickname' : standard.AccountNickname,
                'AccountName' : standard.AccountName,
                'AccountCreated' : standard.AccountCreated,
                'AccountDescription' : standard.AccountDescription,
                'AccountEntities' : standard.AccountEntities,
                'AccountDescriptionHashtag' : standard.AccountDescriptionHashtag,
                'AccountDescriptionURL' : standard.AccountDescriptionURL,
                'AccountDescriptionMention' : standard.AccountDescriptionMention,
                'AccountLocation' : standard.AccountLocation,
                'AccountProfileImageURL' : standard.AccountProfileImageURL,
                'AccountFollowersCount' : standard.AccountFollowersCount,
                'AccountFollowingCount' : standard.AccountFollowingCount,
                'AccountTweetCount' : standard.AccountTweetCount,
                'AccountListedCount' : standard.AccountListedCount,
                'AccountLikeCount' : standard.AccountLikeCount,
                'AccountURL' : standard.AccountURL,
                'AccountVerified' : standard.AccountVerified,
                'Include' : standard.Include,
                'TweetData' : [{
                    'TweetID' : standard.TweetID,
                    'TweetContent' : standard.TweetContent,
                    'AuthorID' : standard.TweetID,
                    'OriginalTweetID' : standard.OriginalTweetID,
                    'TweetCreated' : standard.TweetCreated,
                    'TweetEntities' : standard.TweetEntities,
                    'TweetContentHashtag' : standard.TweetContentHashtag,
                    'TweetContentURL' : standard.TweetContentURL,
                    'TweetMention' : standard.TweetMention,
                    'TweetMedia' : standard.TweetMedia,
                    'TweetMediaURL' : standard.TweetMediaURL,
                    'OriginalTweetAuthorID' : standard.OriginalTweetAuthorID,
                    'TweetLanguage' : standard.TweetLanguage,
                    'TweetRetweetCount' : standard.TweetRetweetCount,
                    'TweetReplyCount' : standard.TweetReplyCount,
                    'TweetLikeCount' : standard.TweetLikeCount,
                    'TweetQuoteCount' : standard.TweetQuoteCount,
                    'TweetBookmarkCount' : standard.TweetBookmarkCount,
                    'TweetImpressionCount' : standard.TweetImpressionCount,
                    'TweetReferencedTweets' : standard.TweetReferencedTweets,
                    'TweetSource' : standard.TweetSource,
                    'TweetPlace' : standard.TweetPlace,
                    'TweetContextAnnotations' : standard.TweetContextAnnotations
                }]
            }

            result = collection.insert_one(data)

            if result.inserted_id:
                print("데이터 생성 완료")
            else:
                print("데이터 생성 실패")



if __name__ == "__main__":
    mongo_handler = MongoDBHandler()
    my_collection = mongo_handler.get_collection('tweet')
    count = 0
    my_collection.create_index([('AccountID')], unique = True)

    csv_file_path = 'C:\\Users\\Cysec\\Desktop\\knowledge_graph\\sampleRetweeted2.csv'
    df = pd.read_csv(csv_file_path, encoding='utf-8-sig')

    for standard in df.itertuples():
        #temp = my_collection.find_one({'TweetData.TweetID' : standard.TweetID})
        temp = my_collection.find_one({'TweetData': {'$elemMatch' : {'TweetID' :standard.TweetID}}})
        if temp:
            print("TweetID already exists in the database: ", standard.TweetID)
            continue
        mongo_handler.insert_document(my_collection, standard)
        count += 1
        print(count)
