# Language Sentiment and AWS Comprehend

Using Amazon Comprehend to add sentiment analysis of text to the tagging pipeline.

## Async Comprehend
Due to the large amount of data that has to be processed the Asynchronous Batch Processing method is being used.

See docs on this for technical details, [here](https://docs.aws.amazon.com/comprehend/latest/dg/how-async.html).

## Setup
For this to work you need to setup:
- grant Amazon Comprehend access to the Amazon S3 bucket that contains your document collection and output files

### Grant access to Amazon Comprehend
Follow the instructions [here](https://docs.aws.amazon.com/comprehend/latest/dg/access-control-managing-permissions.html#auth-role-permissions).

Make sure that your role has: "s3:GetObject", "s3:ListBucket", "s3:PutObject" for the buckets that you will persist your input data to.

### Overview of phoenix process
1. `phoenix/tag/third_party_models/aws_async/start_sentiment.ipynb`:
The notebook needs `objects` data which is created from running the tagging pipeline.
*TODO: Add a link to the document of the pipeline. Currently there is no such document*

- For each language that will be processed a job is created
- For each job an:
  - A folder in the cloud is created
  - A input file is created which has a line for each document (document is the Comprehend term for text). This is persisted in the cloud.
  - The objects that have been processed is persisted in the cloud

- The AWS StartSentimentDetectionJob API endpoint is called
- The meta data about that asynchronous job is persisted. This metadata data is stored in the type AsyncJobGroup.

2. Wait for the Asynchronous job to finish:

This will take around 10 minutes.
You can check the status by going to the [console](https://eu-central-1.console.aws.amazon.com/comprehend/v2/home?region=eu-central-1#analysis)
Or run the `complete_sentiment.ipynb` notebook which will give an error if the job is not complete.

3. `phoenix/tag/third_party_models/aws_async/complete_sentiment.ipynb`:

- Get the persisted metadata
- For each job in the metadata:
  - The DescribeSentimentDetectionJob API endpoint is called
- If any of the jobs are not complete then an error is raises
- If all the jobs are complete then the output that comprehend has created is processed
- The outputs from comprehend are added to all the objects that have been processed creating `language_sentiment_objects`.

4. The `language_sentiment_objects` data can then be used to finalise twitter, facebookposts and facebook comments.
