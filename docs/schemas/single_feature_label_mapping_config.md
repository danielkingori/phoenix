# Single Feature to Label mapping config dataframe

Schema of the Single Feature to Label Mapping (SFLM)

| Column   | dtype  | Description |
|----------|--------|-------------|
| processed_features | string | A word or group of words that are features that point to a label. Created by NLP pipeline with stemming/lemmafication processed. |
| unprocessed_features | string | A word or group of words that are features that point to a label. Features are the original features given by the labeller and left un processed by the NLP pipelne. |
| class    | string | The class that the feature maps to.|
| use_processed_features | bool | Flag to use the processed features or unprocessed features. |
| status | string | Literal["active", "deleted"] Status of the feature to label mapping: active or deleted (by user)|
| language | string | inferred language of the first text that generated this feature|
| language_confidence | float | confidence of the inferred language |
