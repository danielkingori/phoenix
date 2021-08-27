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
| is_key_feature        | bool    | is a relevant feature |
