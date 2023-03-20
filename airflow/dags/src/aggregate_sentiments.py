from typing import *
import pandas as pd

def calculate_sentiment_score(
    sentiments: pd.Series, 
    neg_label: Any = -1, 
    pos_label: Any = 1
) -> List[float]:
    """
    Calculate sentiment score.
    """
    num_pos, num_neg, num_neu = 0, 0, 0
    for i in sentiments:
        if i == neg_label:
            num_neg += 1
        elif i == pos_label:
            num_pos += 1
        else:
            num_neu += 1
    if num_pos == num_neg:
        return 0
    else:
        score = (num_pos - num_neg) / (num_pos + num_neg)
        return [num_neg, num_neu, num_pos, score]


def aggregate_sentiments(
    sentiments_df: pd.DataFrame, 
    time_col: str = 'time_pulled', 
    sent_col: str = 'sentiment', 
    neg_label: Any = -1, 
    pos_label: Any = 1    
) -> pd.DataFrame:
    """
    Return a dataframe of sentiment score at each time point.
    """
    time_pts = sentiments_df[time_col].sort_values().unique()
    res = []
    for t in time_pts:
        tmp = sentiments_df[sentiments_df[time_col] == t]
        sent_agg = calculate_sentiment_score(tmp[sent_col], neg_label, pos_label)
        sent_agg = [t] + sent_agg
        res.append(sent_agg)
    res_df = pd.DataFrame(res, columns=['time', 'num_neg', 'num_neu', 'num_pos', 'score'])
    return res_df
