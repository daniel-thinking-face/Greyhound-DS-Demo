# Data Acquisition

The data acquisition process consists of scraping the Greyhound Board of Great Britain website - www.gbgb.org.uk - daily for race results.

The function is housed in Google Cloud using 3 pieces of GCP architecture:

1. Google Cloud Scheduler
2. Google Pub/Sub
3. Google Cloud Functions

## Cloud Scheduler

This is set up to publish an event (in this case a time trigger) whic will be sent to google pub sub.

Below is a screenshot of the cloud scheduler set up to trigger at 08:00 every day.

![Cloud Scheduler](https://github.com/daniel-thinking-face/Greyhound-DS-Demo/blob/main/Webscraper%20Cloud%20Build/assets/cloud%20scheduler.png)


## Pub/Sub

Now that we have a scheduled signal set up.  We publish this to a topic in Pub/Sub.  The Cluod function will subscribe to this topic using it as a trigger for activation.

![Pub Sub](https://github.com/daniel-thinking-face/Greyhound-DS-Demo/blob/main/Webscraper%20Cloud%20Build/assets/Pub%20Sub.png)

## Cloud Function

In the cloud function, we set what will trigger our function.

![Cloud Function](https://github.com/daniel-thinking-face/Greyhound-DS-Demo/blob/main/Webscraper%20Cloud%20Build/assets/Cloud%20Function.png)

This will then trigger the functions as described in `main.py` built on the necessafy requirements in `requirements.txt`
