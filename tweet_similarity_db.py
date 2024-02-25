import mongodb
import pandas as pd
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm

class Similarity:

    def cos_similarity(self, set1, set2):
        try:
            vectorizer = TfidfVectorizer(stop_words='english')
            tfidf_matrix = vectorizer.fit_transform([set1, set2])
            cosine_sim = cosine_similarity(tfidf_matrix)
            return cosine_sim[0][1]
        except:
            return 'err'

    def jac_similarity(self, set1, set2):
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        similarity = intersection / union
        return similarity

    def hashtag_similarity(self, set1, set2):
        if not set1 or not set2:
            return 0
        set1 = set(set1)
        set2 = set(set2)
        return self.jac_similarity(set1, set2)

    def content_similarity(self, set1, set2):
        if not set1 or not set2:
            return 0
        return self.cos_similarity(set1, set2)

    def content_processing(self, data):
        if not pd.notna(data):
            return ""
        url_del = r'http[s]?://[^\s]+'
        hashtag_del = r'#\w+'
        data = re.sub(url_del, '', data)
        data = re.sub(hashtag_del, '', data)
        data = data.strip()
        return data

    def url_similarity(self, set1, set2):
        if not set1 or not set2:
            return 0
        set1 = set(set1)
        set2 = set(set2)
        intersection = set1.intersection(set2)
        if intersection:
            return 1
        else:
            return 0



if __name__ == "__main__":
    mongo_handler = mongodb.MongoDBHandler()
    collection = mongo_handler.get_collection('tweet')
    sim = Similarity()
    result = list(collection.find())


    for index, standard in tqdm(enumerate(result), desc = "Data Processing", unit = '%',total = len(result)):
        for standard_tweet_data in standard['TweetData']:
            for comparison in result[index + 1:]:
                account_sim_data = {
                        'ComparisonAccountID' : comparison['AccountID'],
                        'AccountHashtagSimilarity' : sim.hashtag_similarity(standard['AccountDescriptionHashtag'], comparison['AccountDescriptionHashtag']),
                        'AccountContentSimilarity' : sim.content_similarity(standard['AccountDescriptionPreprocessing'], comparison['AccountDescriptionPreprocessing']),
                        'AccountURLSimilarity' : sim.url_similarity(standard['AccountDescriptionURL'], comparison['AccountDescriptionURL'])
                        }
                for comparison_tweet_data in comparison['TweetData']:
                    tweet_sim_data = {
                        'ComparisonTweetID' : comparison_tweet_data['TweetID'],
                        'TweetContentSimilarity' :sim.content_similarity(standard_tweet_data['TweetContentPreprocessing'], comparison_tweet_data['TweetContentPreprocessing']),
                        'TweetHashtagSimilarity' : sim.hashtag_similarity(standard_tweet_data['TweetContentHashtag'], comparison_tweet_data['TweetContentHashtag']),
                        'TweetURLSimilarity' : sim.url_similarity(standard_tweet_data['TweetContentURL'], comparison_tweet_data['TweetContentURL'])
                        }
                    #print(standard['AccountDescriptionHashtag'])
                    #print(comparison['AccountDescriptionHashtag'])
                    #print(standard['AccountDescriptionPreprocessing'])
                    #print(comparison['AccountDescriptionPreprocessing'])
                    #print(standard_tweet_data['TweetContentPreprocessing'])
                    #print(comparison_tweet_data['TweetContentPreprocessing'])
                    #print(standard_tweet_data['TweetContentHashtag'])
                    #print(comparison_tweet_data['TweetContentHashtag'])

                    #print('standard_tweetID : {}    comparison_tweetID {}'.format(standard_tweet_data['TweetID'], comparison_tweet_data['TweetID']))
                    #print('TweetContentSimilarity : {}   TweetHashtagSimilarity " {}'.format(tweet_sim_data['TweetContentSimilarity'], tweet_sim_data['TweetHashtagSimilarity']))
                    #print(account_sim_data['AccountURLSimilarity'], tweet_sim_data['TweetURLSimilarity'], account_sim_data['AccountContentSimilarity'], account_sim_data['AccountHashtagSimilarity'], tweet_sim_data['TweetContentSimilarity'], tweet_sim_data['TweetHashtagSimilarity'])

            
            
            


    #temp = collection.find({'TweetData': {'$elemMatch' : {'TweetID' : standard.TweetID}}})





