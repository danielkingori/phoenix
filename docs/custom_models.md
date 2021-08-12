# Custom models

We build or retrain custom models to classify or predict things about the data that we have.

## Tension Classifier

This model predicts whether or not a tension (eg economic labour tensions) is present within an
 object. See [object_tensions](schemas/objects.md#object_tensions-dataframe) for expected tensions.

The classifier pulls from manually annotated data by an analyst for being about a certain tension
, and if there are any features that are highly correlated for the object being about that tension.
 
The training of the model is done in this module, and persisted as a pickle file. 
The performance and training states of these models are logged through `kotsu` 
The better model is then
 picked up by the `phoenix/tag/tag_tensions.ipynb` notebook and used for classifying tensions
  and tagging objects with that tension.