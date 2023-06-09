"""Generate labelling sheet for human labelers."""
from typing import Tuple

import pandas as pd
from sklearn.model_selection import train_test_split


EXPECTED_COLUMNS_OBJECT_LABELLING_SHEET = [
    "object_id",
    "object_type",
    "object_url",
    "created_at",
    "object_user_url",
    "matched_labels",
    "matched_features",
    "text",
    "labelled_by",
    "label_1",
    "label_1_features",
    "label_2",
    "label_2_features",
    "label_3",
    "label_3_features",
    "label_4",
    "label_4_features",
    "label_5",
    "label_5_features",
    "notes",
]

EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET = [
    "object_user_url",
    "object_user_name",
    "labelled_by",
    "account_label_1",
    "account_label_2",
    "account_label_3",
    "account_label_4",
    "account_label_5",
]


def create_object_labelling_df(
    for_tag_df: pd.DataFrame, with_user_notes: bool = True
) -> pd.DataFrame:
    """Create a new object labeling dataframe using the for_tagging pulled data.

    Args:
        for_tag_df (pd.DataFrame): dataframe with data from the tag/data_pull scripts.
        with_user_notes (bool): flag, if True get and add user notes to output dataframe
    """
    for col in EXPECTED_COLUMNS_OBJECT_LABELLING_SHEET:
        if col not in for_tag_df.columns:
            for_tag_df[col] = None

    for_tag_df = for_tag_df[EXPECTED_COLUMNS_OBJECT_LABELLING_SHEET]

    for_tag_df["created_at"] = for_tag_df["created_at"].apply(
        lambda x: pd.to_datetime(x).strftime("%Y-%m-%d %H:%M") if x else None
    )

    if with_user_notes:
        user_notes_df = get_user_notes_object_df()
        for_tag_df = user_notes_df.append(for_tag_df)

    for_tag_df["text"] = for_tag_df["text"].astype(str)

    return for_tag_df


def create_account_labelling_dataframe(
    for_tag_df: pd.DataFrame, with_user_notes: bool = True
) -> pd.DataFrame:
    """Create a new account labelling dataframe using the for_tagging pulled data.

    Args:
        for_tag_df (pd.DataFrame): dataframe with data from the tag/data_pull scripts.
        with_user_notes (bool): Should the first row of the df be notes for the user of the sheet?
    """
    if with_user_notes:
        user_notes_df = get_user_notes_account_df()
    else:
        user_notes_df = pd.DataFrame(columns=EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET)

    deduped_account_df = for_tag_df[["object_user_url", "object_user_name"]].drop_duplicates()
    for col in EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET:
        if col not in deduped_account_df.columns:
            deduped_account_df[col] = None

    return user_notes_df.append(deduped_account_df[EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET])


def get_user_notes_object_df() -> pd.DataFrame:
    """Adds notes for the users of the object_labelling_sheet as the first row of a df."""
    notes_list = [
        # Note for column: object_id
        "Internal ID",
        # Note for column: object_type
        "Type of object",
        # Note for column: object_url
        "Object's url. Click to see context of the object",
        # Note for column: created_at",
        "Date of posting",
        # Note for column: object_user_url
        "User url",
        # Note for column: matched_labels
        "The class labels the system has output for the object",
        # Note for column: matched_features
        "The features the system used to identify which labels to apply to object",
        # Note for column: text
        "The content of the object",
        # Note for column: labelled_by
        "Who is labelling this post? (name of the person not the org)",
        # Note for column: label_1
        "What's the class?",
        # Note for column: label_1_features
        "The Features (i.e.: keywords, words or phrases) that made us think that it is this "
        "class (and always think it is this class)",
        # Note for column: label_2
        "Is there another class mentioned here as well?",
        # Note for column: label_2_features
        "The Features (i.e.: keywords, words or phrases) that made us think that it is this "
        "class (and always think it is this class)",
        # Note for column: label_3
        "Is there another class mentioned here as well? if not, please leave empty.",
        # Note for column: label_3_features
        "The Features (i.e.: keywords, words or phrases) that made us think of this class if not, "
        "please leave empty",
        # Note for column: label_4
        "Is there another class mentioned here as well? if not, please leave it empty.",
        # Note for column: label_4_features
        "the Features (i.e.: keywords, words or phrases) that made us think of this class if not, "
        "please leave empty",
        # Note for column: label_5
        "Is there another class mentioned here as well? if not, please leave it empty.",
        # Note for column: label_5_features
        "the Features (i.e.: keywords, words or phrases) that made us think of this class if not, "
        "please leave empty",
        # Note for column: notes
        "Space to add your own notes.",
    ]

    data_dict = dict(zip(EXPECTED_COLUMNS_OBJECT_LABELLING_SHEET, notes_list))
    df = pd.DataFrame(data=data_dict, columns=EXPECTED_COLUMNS_OBJECT_LABELLING_SHEET, index=[0])
    return df


ACCOUNT_NOTES_LIST = [
    "User's URL. Click to see the context of this user",
    "Username of this user's account.",
    "Who is labelling this post? (name of the person not the org)",
    "What's the class?",
    "Is there another class mentioned here as well? If not, please leave empty",
    "Is there another class mentioned here as well? If not, please leave empty",
    "Is there another class mentioned here as well? If not, please leave empty",
    "Is there another class mentioned here as well? If not, please leave empty",
]


def get_user_notes_account_df() -> pd.DataFrame:
    """Adds notes for the users of the account_labelling sheet as the first row of a df."""
    data_dict = dict(zip(EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET, ACCOUNT_NOTES_LIST))
    df = pd.DataFrame(data=data_dict, columns=EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET, index=[0])
    return df


def get_goal_number_rows(
    df: pd.DataFrame, stratify_col: str, n: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Stratified split a dataframe based on a column to get a goal number of rows.

    Args:
        df (pd.DataFrame): dataframe to split
        stratify_col (str): which column do you want to use to ensure a representative number
            when splitting
        n (int): Target number to get
    Returns:
        pd.DataFrame: excluded rows
        pd.DataFrame: included rows
    """
    if n < 0:
        excluded_df = df.copy()
        df = pd.DataFrame(columns=df.columns)
    elif df[stratify_col].nunique() > n:
        # If there are more unique items in the stratify columns than `n`, do a simple sample
        df_copy = df.copy()
        df = df.sample(n, random_state=2021)
        excluded_df = df_copy[~df_copy["object_id"].isin(df["object_id"])]
    elif len(df) > n:
        test_size = n / len(df)
        # remove rows when the stratified column only has one object; needed for train_test_split
        no_non_duplicates_df = df[df[stratify_col].duplicated(keep=False)]
        excluded_df, df = train_test_split(
            no_non_duplicates_df,
            test_size=test_size,
            stratify=no_non_duplicates_df[[stratify_col]],
            random_state=42,
        )
        excluded_df.append(df[~df[stratify_col].duplicated(keep=False)])
    else:
        excluded_df = pd.DataFrame(columns=df.columns)

    return excluded_df, df
