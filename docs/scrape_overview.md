# Scrape overview
This document outlines the how and what sources are scrape by phoenix.

The sources that can be scraped and processed are:
- Facebook posts
- Tweets
- Facebook comments

## Facebook Posts
The facebook posts are scraped using the CrowdTangle API. 
This pulls data from facebook and requires permission from CrowdTangle to use.
This guide does not cover how to get access to CrowdTangle. 

The Facebook post data will come from a `list` on Crowdtangle.
Follow the Crowdtangle documentation to set up this list.
We use the `list IDs` to identify which list to pull on Crowdtangle.
These should be set on a per tenant basis, see `phoenix/config/tenants_template.yaml`.

Set the following environment variables:

`CROWDTANGLE_API_TOKEN`

`CT_RATE_LIMIT_CALLS` _(unless using default rate limit)_


To run the scrape, use the following command:
```bash 
$ ./phoenix-cli fb $(date --utc --iso-8601=seconds)
```

The facebook posts source run will pull the posts for the previous 3 days from the CrowdTangle API.
Previous 3 days because this is a balance between size of data pulled and accuracy of interactions with the post.

Data on the interactions on a post such as likes and comments change over time. In general the interactions change only slightly after 3 days.

Pulling only three days of data means that we are within the limits of what Facebook allows us to request.

The scraped data will conform to the following [Facebook posts schema](docs/facebook_posts_table.md) within Athena.

### CrowdTangle API
Facebook provides a [CrowdTangle API cheatsheet](https://help.crowdtangle.com/en/articles/3443476-api-cheat-sheet).
Here is the full [CrowdTangle API documentation](https://github.com/CrowdTangle/API/wiki).

#### Rate Limits
CrowdTangle has a base rate limit of 6 calls / minute.
We recommend asking to increase your Crowdtangle API rate limit with [a form on their site](https://www.facebook.com/help/contact/908993259530156).

We have found success with a limit of 30 calls / minute.


## Tweets
Tweets are pulled from the twitter APIs using the `user timeline` and `keyword` endpoints.

You must have a Twitter developer account.
For more information on the Twitter API, see [Twitter API documentation](https://developer.twitter.com/en/docs).

### Configuration
Two configuration CSV files are used for the scraping.

1. `config/twitter_query_users.csv` - user handles to scrape, a new line per user handle
2. `config/twitter_query_keywords.csv` - keywords to scrape, single or multiple keywords per line

For local development these should be stored in `local_artifacts/config/`.
For production these should be stored in `<production url>/config/`.

**Important!:** Phoenix is currently set up to use Twitter API V1. 
Please follow all API guidance from Twitter to avoid any problems with your access.
These tools are designed within the boundaries of the API guidelines, 
and the Phoenix team is not responsible for any misuse.

### Authentication
There are two methods of authentication. As suggested in the `tweepy` documentation found [here](https://docs.tweepy.org/en/stable/auth_tutorial.html) use OAuth 1 or 2.

For OAuth 1 use environment variables:
-`TWITTER_CONSUMER_KEY`
-`TWITTER_CONSUMER_SECRET`
-`TWITTER_OAUTH_ACCESS_TOKEN`
-`TWITTER_OAUTH_ACCESS_SECRET`

For OAuth 2 use environment variables:
- `TWITTER_APPLICATION_KEY`
- `TWITTER_APPLICATION_SECRET`

### CLI
To see information on cli:
`./phoenix-cli scrape tweets --help`


### General
A source run collects the previous 3 days tweets.
This is in line with our approach for Facebook.
Our expectation is that most activity around individual tweets reduces after the first few days.

The Twitter data will conform to the following [Tweet schema](docs/tweets_table.md).

## Facebook comments
The facebook comments pipeline is more complex and includes a mix of manual and automatic processes.

The process has the ordered steps:

**1. Pull facebook posts for a month** - run the Facebook post process.

**2. Find top 10% of relevant posts by doing a tagging run** - 
The current implementation outputs a `posts_to_scrape.csv`, see [notebook](/phoenix/tag/twitter_facebook_posts_finalise.ipynb).

**3. Do a manual collection on the comments for each post** - 
following this tutorial on [collecting comments from Facebook pages](docs/facebook-comment-collection.md). 

**4. Upload all collected html pages to the proper `fb_comments/to_parse` cloud folder.** 

**5. Run a script that will process the html pages** and transform them into more structured data (json) using the following terminal command:
```bash
$ ./phoenix-cli fb-comments $(date --utc --iso-8601=seconds)
```

**6. Send the structured comments data through the tagging pipeline** so that the heuristics about that data can also be found. 
Here is [documentation on the Facebook Comments schema](docs/facebook_comments_table.md).

### Disclaimer

This approach to collecting Facebook comments is experimental and not officially sanctioned by Facebook. 
Use your own discretion when following this methodology.
If this approach is abused, it could result in blocked accounts or other forms of reprimand from Facebook.
While we have tested this approach in our own projects and found it to be valuable and viable, 
the Phoenix team is not responsible for blocked accounts. 

## Data Protection
It is up to you to follow the appropriate data protection guidelines as laid out in your jurisdiction. 