# Twitter-Sentiment-Analysis

Spark Streaming application to read tweets from specific topic ('summer'). Then, use the tweets to analyze sentiment and view results in real time using kafka and elastic-search. 

Sentiment values: "Very negative" = 0, "Negative" = 1, "Neutral" = 2, "Positive" = 3, "Very positive" = 4

Results: After streaming and analyzing tweets that contain the word 'summer' for a few hours, the following graphs were generated.

![alt text](/graphical_plots/1_sentiment_over_time.png)
