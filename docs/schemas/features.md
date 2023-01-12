# Features dataframe

The Features dataframe has a row for every feature in the objects dataframe.

This means that the dtypes are the same apart from the `features` and `features_count`.

The `features_final` in `features.ipynb` has more variables but these had to be cut down due to the large amount of features that are created.

| Column                | dtype   | Description |
|-----------------------|---------|-------------|
| object_id             | object  | |
| object_type           | object  | |
| features              | object  | The feature |
| features_count        | int64   | The number of times the feature appears in the object |

# SFLM Necessary Features dataframe

The Features dataframe is large and has unncessary columns of duplicated data. The text of each
 object is duplicated by the number of unique features it has. 
 
 A long text from facebook_posts of 300 (unique) words and 2000 characters run with the ngram_range 
 (1,3) will have order of `300(words) x 3 (ngram_range) = 900 duplications` of the original object.
  Each char is 2 bytes for unicode, and the text is duplicated in `clean_text` and `text
  ` columns, making the object `900 duplications x 2000 chars x 2 bytes x 2 cols = 7.2MB
  ` without counting any of the other columns.
      
This sometimes causes memory issues. The SFLM necessary features dataframe only has the
 necessary columns to run the SFLM pipeline.
  
| Column                | dtype   | Description |
|-----------------------|---------|-------------|
| object_id             | object  | |
| object_type           | object  | |
| language              | object  | The inferred language of the object |
| features              | object  | The feature |
