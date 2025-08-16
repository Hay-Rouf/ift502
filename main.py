import tweepy
import pandas as pd
import json
import time
from datetime import datetime, timedelta
import re
import csv

api_key = "jSYG05grvX0dv9dcFRKbgjq5p"
api_secret = "JMTZ18b1bRX0lY5mD8DQl3vZ7IBCLOpn9Euw85GAsoQVRqP5eB"
bearer_token = "AAAAAAAAAAAAAAAAAAAAAPUz3gEAAAAA3Xgb70EzsfDiX292J1U%2FOUCAnfw%3DDNZJ7oCae6oW8aN128S3VVS4FjBXXDMc5LSoefD9RUsF2Fgxwr"
access_token="1322808036969295872-VjZsAMqh3W9ysbaRIefBR4tB8FCSrX"
access_token_secret = "Vxu9w00FBDJDUpOmwFzGMy5xbsTNLy7yH4YF7R9XM0F72"

class JAMBTwitterCollector:
    def __init__(self, bearer_token, api_key=None, api_secret=None, access_token=None, access_token_secret=None):
        """
        Initialize Twitter API clients
        For Academic Research Track, you mainly need bearer_token
        """
        self.bearer_token = bearer_token

        # Twitter API v2 client (recommended)
        self.client = tweepy.Client(bearer_token=bearer_token)

        # Twitter API v1.1 client (for some legacy features)
        if all([api_key, api_secret, access_token, access_token_secret]):
            auth = tweepy.OAuthHandler(api_key, api_secret)
            auth.set_access_token(access_token, access_token_secret)
            self.api = tweepy.API(auth, wait_on_rate_limit=True)

    def get_jamb_keywords(self):
        """
        Define JAMB-related keywords including Nigerian context
        """
        keywords = [
            # Core JAMB terms
            "JAMB", "UTME", "Joint Admissions Matriculation Board",
            "JAMB result", "UTME result", "JAMB score", "UTME score",

            # Nigerian university context
            "university admission", "post utme", "cutoff mark",
            "admission list", "JAMB registration", "JAMB form",

            # Common hashtags (update yearly)
            "#JAMB2024", "#UTME2024", "#JAMBResult2024",
            "#PostUTME", "#UniversityAdmission", "#JAMBScore",

            # Nigerian Pidgin and local expressions
            "jamb don come out", "my jamb score", "jamb registration",
            "admission don come", "cutoff don high"
        ]
        return keywords

    def build_search_query(self, keywords_list, additional_filters=None):
        """
        Build Twitter search query with operators
        """
        # Combine keywords with OR operator
        base_query = " OR ".join([f'"{keyword}"' for keyword in keywords_list])

        # Add location filter for Nigeria
        location_filter = " (place:nigeria OR profile_location:nigeria)"

        # Add language filters
        lang_filter = " lang:en"  # English tweets (includes Nigerian English)

        # Exclude retweets to get original content
        no_retweets = " -is:retweet"

        # Build final query
        query = f"({base_query}){location_filter}{lang_filter}{no_retweets}"

        if additional_filters:
            query += f" {additional_filters}"

        return query

    def collect_recent_tweets(self, max_results=100, days_back=7):
        """
        Collect recent JAMB-related tweets
        """
        keywords = self.get_jamb_keywords()
        query = self.build_search_query(keywords)

        # Calculate date range
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days_back)

        tweets_data = []

        try:
            tweets = tweepy.Paginator(
                self.client.search_recent_tweets,
                query=query,
                max_results=max_results,
                start_time=start_time,
                end_time=end_time,
                tweet_fields=['created_at', 'author_id', 'public_metrics', 'context_annotations', 'lang'],
                user_fields=['username', 'location', 'verified', 'public_metrics']
            ).flatten(limit=max_results)

            for tweet in tweets:
                tweet_data = {
                    'id': tweet.id,
                    'text': tweet.text,
                    'created_at': tweet.created_at,
                    'author_id': tweet.author_id,
                    'retweet_count': tweet.public_metrics['retweet_count'],
                    'like_count': tweet.public_metrics['like_count'],
                    'reply_count': tweet.public_metrics['reply_count'],
                    'quote_count': tweet.public_metrics['quote_count'],
                    'language': tweet.lang
                }
                tweets_data.append(tweet_data)

        except Exception as e:
            print(f"Error collecting tweets: {e}")

        return pd.DataFrame(tweets_data)

    def collect_historical_tweets(self, start_date, end_date, max_results=500):
        """
        Collect historical tweets (requires Academic Research Track)
        """
        keywords = self.get_jamb_keywords()
        query = self.build_search_query(keywords)

        tweets_data = []

        try:
            tweets = tweepy.Paginator(
                self.client.search_all_tweets,  # Academic Research only
                query=query,
                start_time=start_date,
                end_time=end_date,
                max_results=100,  # Max per request
                tweet_fields=['created_at', 'author_id', 'public_metrics', 'context_annotations', 'lang', 'geo'],
                user_fields=['username', 'location', 'verified', 'public_metrics'],
                place_fields=['country', 'name', 'full_name']
            ).flatten(limit=max_results)

            for tweet in tweets:
                tweet_data = {
                    'id': tweet.id,
                    'text': tweet.text,
                    'created_at': tweet.created_at,
                    'author_id': tweet.author_id,
                    'retweet_count': tweet.public_metrics['retweet_count'],
                    'like_count': tweet.public_metrics['like_count'],
                    'reply_count': tweet.public_metrics['reply_count'],
                    'quote_count': tweet.public_metrics['quote_count'],
                    'language': tweet.lang
                }
                tweets_data.append(tweet_data)

        except Exception as e:
            print(f"Error collecting historical tweets: {e}")
            print("Note: Historical search requires Academic Research Track access")

        return pd.DataFrame(tweets_data)

    def stream_live_tweets(self, duration_minutes=60):
        """
        Stream live JAMB-related tweets
        """

        class JAMBStreamListener(tweepy.StreamingClient):
            def __init__(self, bearer_token, output_file):
                super().__init__(bearer_token)
                self.output_file = output_file
                self.start_time = time.time()
                self.duration = duration_minutes * 60

            def on_tweet(self, tweet):
                # Check if duration exceeded
                if time.time() - self.start_time > self.duration:
                    self.disconnect()
                    return False

                tweet_data = {
                    'id': tweet.id,
                    'text': tweet.text,
                    'created_at': tweet.created_at,
                    'author_id': tweet.author_id,
                    'timestamp': datetime.now().isoformat()
                }

                # Save to file
                with open(self.output_file, 'a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    writer.writerow([tweet_data['id'], tweet_data['text'],
                                     tweet_data['created_at'], tweet_data['author_id']])

                print(f"Collected tweet: {tweet.text[:50]}...")
                return True

        # Initialize stream
        stream = JAMBStreamListener(self.bearer_token, "live_jamb_tweets.csv")

        # Add rules for JAMB-related content
        keywords = self.get_jamb_keywords()[:25]  # Twitter allows max 25 keywords per rule
        rule = " OR ".join(keywords)

        try:
            stream.add_rules(tweepy.StreamRule(rule))
            print(f"Starting live stream for {duration_minutes} minutes...")
            stream.filter(tweet_fields=['created_at', 'author_id', 'public_metrics'])
        except Exception as e:
            print(f"Error in streaming: {e}")

    def preprocess_collected_data(self, df):
        """
        Clean and preprocess collected tweet data
        """
        # Remove duplicates
        df = df.drop_duplicates(subset=['text'])

        # Clean text
        df['cleaned_text'] = df['text'].apply(self.clean_tweet_text)

        # Extract JAMB scores if mentioned
        df['mentioned_score'] = df['text'].apply(self.extract_jamb_score)

        # Add time-based features
        df['hour'] = pd.to_datetime(df['created_at']).dt.hour
        df['day_of_week'] = pd.to_datetime(df['created_at']).dt.day_name()

        return df

    def clean_tweet_text(self, text):
        """
        Clean tweet text for analysis
        """
        # Remove URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)

        # Remove user mentions and hashtags for clean analysis
        text = re.sub(r'@\w+|#\w+', '', text)

        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    def extract_jamb_score(self, text):
        """
        Extract JAMB scores mentioned in tweets
        """
        # Look for patterns like "scored 280", "got 245", "jamb score: 267"
        score_patterns = [
            r'scored?\s*(\d{2,3})',
            r'got\s*(\d{2,3})',
            r'score[d:]*\s*(\d{2,3})',
            r'jamb\s*(\d{2,3})',
            r'utme\s*(\d{2,3})'
        ]

        for pattern in score_patterns:
            match = re.search(pattern, text.lower())
            if match:
                score = int(match.group(1))
                # Validate JAMB score range (typically 0-400)
                if 0 <= score <= 400:
                    return score

        return None

    def save_data(self, df, filename, format='csv'):
        """
        Save collected data to file
        """
        if format.lower() == 'csv':
            df.to_csv(f"{filename}.csv", index=False, encoding='utf-8')
        elif format.lower() == 'json':
            df.to_json(f"{filename}.json", orient='records', indent=2)
        elif format.lower() == 'excel':
            df.to_excel(f"{filename}.xlsx", index=False)

        print(f"Data saved as {filename}.{format}")


# Example usage
def main():
    # Initialize collector with your Twitter API credentials
    collector = JAMBTwitterCollector(
        bearer_token="YOUR_BEARER_TOKEN",
        # Add other credentials if you have them
    )

    # Collect recent tweets
    print("Collecting recent JAMB tweets...")
    recent_tweets = collector.collect_recent_tweets(max_results=500, days_back=7)

    if not recent_tweets.empty:
        # Preprocess data
        recent_tweets = collector.preprocess_collected_data(recent_tweets)

        # Save data
        collector.save_data(recent_tweets, "jamb_tweets_recent", format='csv')

        # Display basic statistics
        print(f"Collected {len(recent_tweets)} tweets")
        print(f"Date range: {recent_tweets['created_at'].min()} to {recent_tweets['created_at'].max()}")

        # Show tweets with scores mentioned
        score_tweets = recent_tweets[recent_tweets['mentioned_score'].notna()]
        if not score_tweets.empty:
            print(f"Found {len(score_tweets)} tweets mentioning JAMB scores")
    else:
        print("No tweets collected. Check your API credentials and search parameters.")


if __name__ == "__main__":
    main()