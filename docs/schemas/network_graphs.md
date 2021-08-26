# Network Maps

## Twitter

### Retweet Graph
Dataframe for retweets graph.

| Column               | Datatype | Description                                               |
|----------------------|----------|-----------------------------------------------------------|
| original_screen_name | object   | screen name of original tweet                             |
| retweet_screen_name  | object   | screen name of retweeter                                  |
| count                | int64    | count of retweets between these nodes                     |
| original_listed      | bool     | `True` if original screen_name in user search parameters  |
| retweet_listed       | bool     | `True` if retweeter screen_name in user search parameters |


### Friends Graph
Dataframe for friends graph

| Column             | Datatype | Description                                               |
|--------------------|----------|----------------------------------------------|
| user_1  | object   | screen name of user account                             |
| user_2  | object   | screen name of person followed by user                  |

