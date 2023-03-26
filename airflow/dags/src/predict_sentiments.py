from typing import *
import re
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import pandas as pd
import pendulum


def process_tweet(tweet: str):
    """
    Clean tweets before sentiment prediction.
    """
    tweet = re.sub(r'[#@]\S+', '', tweet)
    tweet = re.sub(r'http\S+', '', tweet)
    tweet = ' '.join(tweet.split())
    return tweet


def transformer_predict(tweets: List[str], verbose: bool = False) -> List[int]:
    """
    Return predicted probabilities and labels.
    """
    ID_TO_LABEL = {
        0: 1, # "positive"
        1: -1, # "negative",
        2: 0, #"neutral"
    }
    device = "cuda:0" if torch.cuda.is_available() else "cpu"
    # load tuned tokenizer and model
    tokenizer = AutoTokenizer.from_pretrained('ProsusAI/finbert')
    model = AutoModelForSequenceClassification.from_pretrained('ProsusAI/finbert')
    model = model.to(device)
    with torch.no_grad():
        # tokenize
        inputs = tokenizer(tweets, truncation=True, padding=True, return_tensors="pt").to(device)
        # predict
        outputs = model(**inputs)
        # apply softmax
        softmax = torch.softmax(outputs.logits, dim=-1)
        preds = softmax.argmax(dim=-1, keepdim=False)
        # relabel prediction
        preds = pd.Series(preds).map(ID_TO_LABEL).tolist()
    if verbose:
        print('Completed sentiment prediction.')
    return preds


def get_tweets_sentiments(
    tweets_list: List[Dict], 
    text_field: str = 'content',
) -> List[int]:
    """
    Process tweets and predict sentiments.
    """
    tweets = []
    for dic in tweets_list:
        processed_t = process_tweet(dic[text_field])
        tweets.append(processed_t)
    sentiments = transformer_predict(tweets)
    return sentiments


def generate_tweets_df(
    tweets_list: List['Dict'],
    sentiments: List[int],
    date_field: str = 'date',
    time_pulled_field: str = 'time_pulled'
) -> pd.DataFrame:
    """
    Combine the scraped tweets and predicted sentiments into a dataframe.
    """
    tweets_df = pd.DataFrame(tweets_list)
    # append sentiments
    tweets_df['sentiment'] = sentiments
    # convert datetime string to datetime
    tweets_df[date_field] = tweets_df[date_field].apply(lambda x: pendulum.parse(x))
    tweets_df[time_pulled_field] = tweets_df[time_pulled_field].apply(lambda x: pendulum.parse(x))
    return tweets_df