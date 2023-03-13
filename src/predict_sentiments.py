from typing import *
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import pandas as pd


def process_tweet(tweet: str):
    """
    Remove url and trailing whitespace
    """
    tokens = tweet.split()
    tokens = tokens[:-1]
    tweet = ' '.join(tokens)
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


def get_tweets_sentiment(tweets_df: pd.DataFrame, id_col: str, text_col: str) -> pd.DataFrame:
    """
    Return a dataframe of tweet ids and corresponding sentiments.
    """
    ids = tweets_df[id_col].tolist()
    processed_tweets = tweets_df[text_col].apply(process_tweet).tolist()
    sentiments = transformer_predict(processed_tweets)
    sentiments_df = pd.DataFrame({f'{id_col}': ids, 'sentiment': sentiments})
    return sentiments_df


