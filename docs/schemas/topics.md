# Topics dataframe

The topics is the dataframe topics computed for the objects.

| Column                | dtype   | Description |
|-----------------------|---------| ------------|
| object_id             | object  | The unique object id |
| object_type           | object  | Enum based on `phoenix/tag/data_pull/constants.py` |
| topic                 | object  | Computed topic for the text |
| matched_features      | object  | a list of string that are features the matched for the topic |
