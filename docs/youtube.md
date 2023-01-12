# YouTube
It is possible to use phoenix to scrape source data from the YouTube API.

## Scraping configuration
For the scraping of YouTube you will need to:
- Create a Google Account if you don't have one
- Create a project in the Google developer Console
- Enable YouTube API for that project
- Create an API key as your authorization credentials
- Set this API key as `YOUTUBE_API_KEY` in your environment

More information on this can be found:
- https://developers.google.com/youtube/v3/getting-started

## Scrape from channels
To scrape source data about about a channel, and its videos and comments from a list of channel IDs you will need to:
- Get a list of channels and their IDs
- Save this as a CSV in `<tenant_id_folder>/config/youtube_channels.csv`
- Run the cli `./phoenix-cli scrape youtube production <tenant_id> channels_from_channel_ids`

### Getting the IDs of channels
Once you have found a channel that you would like to scrape you will need to get the ID of that channel.
It is not trivial to get the ID from the website of the channel. Here are some resources to help:
- https://commentpicker.com/youtube-channel-id.php
- https://stackoverflow.com/questions/14366648/how-can-i-get-a-channel-id-from-youtube

### Format of the configuration CSV
The file `<tenant_id_folder>/config/youtube_channels.csv` must contain the column: `channel_id`.
