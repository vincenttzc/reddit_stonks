from nltk.sentiment.vader import SentimentIntensityAnalyzer


class VaderSentimentAnalyzer:
    def __init__(self):
        self.vader = SentimentIntensityAnalyzer()

    def calculate_sentiment(self, data):
        data["sentiment_score"] = data["body"].apply(
            lambda text: self.vader.polarity_scores(text)["compound"]
        )
        data["weighted_sentiment_score"] = data["score"] * data["sentiment_score"]

        return data
