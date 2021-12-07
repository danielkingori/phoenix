"""Object Classes."""
import pandas as pd

from phoenix.tag.classification import object_classes


def test_join():
    """Test join of classes dataframe and objects dataframe."""
    classes_df = pd.DataFrame(
        {
            "object_id": ["o1", "o1", "o2", "o2", "o3"],
            "class": ["t1", "t2", "t1", "t2", "other"],
            "object_type": ["ot1", "ot1", "ot1", "ot1", "ot2"],
            "matched_features": [["f1"], ["f1", "f2"], ["f1"], ["f1"], None],
            "has_class": [True, True, True, True, False],
        }
    )

    objects_df = pd.DataFrame(
        {
            "object_id": ["o1", "o2", "o3"],
            "object_type": ["ot1", "ot1", "ot2"],
            "objects_col": ["i", "i", "i"],
        }
    )

    objects_classes_df = object_classes.join(classes_df, objects_df)

    pd.testing.assert_frame_equal(
        objects_classes_df,
        pd.DataFrame(
            {
                "object_id": ["o1", "o2", "o3"],
                "object_type": ["ot1", "ot1", "ot2"],
                "objects_col": ["i", "i", "i"],
                "classes": [["t1", "t2"], ["t1", "t2"], ["other"]],
                "has_classes": [True, True, False],
            }
        ),
    )
