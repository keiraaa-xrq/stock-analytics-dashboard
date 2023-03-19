from typing import *
import re
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import pandas as pd


def process_tweets(tweet: str):
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


def get_tweets_sentiment(
    tweets_df: pd.DataFrame, 
    id_col: str = 'tweet_id', 
    time_col: str = 'time_pulled', 
    text_col: str = 'content'
) -> pd.DataFrame:
    """
    Return a dataframe of tweet ids, scraped time and corresponding sentiments.
    """
    ids = tweets_df[id_col].tolist()
    time = tweets_df[time_col].tolist()
    tweets = tweets_df[text_col].apply(process_tweets).tolist()
    sentiments = transformer_predict(tweets)
    sentiments_df = pd.DataFrame({f'{id_col}': ids, f'{time_col}': time, 'sentiment': sentiments})
    return sentiments_df