"""Utility functions for prodigy text annotation."""
from typing import Any, Dict, List

import json

import pandas as pd
import tentaclio


def span_to_text_snippets(span_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract the text representations of spans with their labels."""
    snippets = []
    if not span_json.get("spans", ""):
        raise Exception("No spans in span_json. Check if spans are correct.")
    if not span_json.get("text", ""):
        raise Exception("No text in span_json. Check if spans are correct.")

    span_text = span_json.get("text")
    for span in span_json.get("spans"):
        start_index = span.get("start")
        end_index = span.get("end")
        snippets.append({"label": span.get("label"), "text": span_text[start_index:end_index]})

    return snippets


def create_spacy_pattern_json(label: str, text: str) -> Dict[str, Any]:
    """Create a single pattern json for spacy models. Splits text on whitespace."""
    pattern = []
    for token in text.split():
        pattern.append({"lower": token})
    pattern_json = {"label": label.lower(), "pattern": pattern}
    return pattern_json


def text_snippet_to_patterns(
    label: str, text_snippets: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Turn text snippets of annotated spans into a patterns file for prodigy training.

    Assumes that any text in the text_snippets need to be split into tokens and lowered.

    label (str): User's label for any text that matches the patterns in the text_snippets.
    text_snippets (List[Dict[str, Any]]): text snippets that constitute a pattern for matching
        text to labels.
    """
    patterns_list = []
    for snippet in text_snippets:
        snippet_text = snippet.get("text")
        pattern_json = create_spacy_pattern_json(label, snippet_text)
        if pattern_json not in patterns_list:
            patterns_list.append(pattern_json)

    return patterns_list


def sfm_to_patterns(sfm_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Transform Single Feature Mapping config file to spacy patterns file."""
    sfm_df["pattern_json"] = sfm_df.apply(
        lambda x: create_spacy_pattern_json(x["topic"], x["features"]), axis=1
    )
    return list(sfm_df["pattern_json"])


def save_dicts_to_jsonl(data_list: List[Dict[str, Any]], filepath: str) -> None:
    """Save list of dicts into newline delimited json file.

    Args:
        data_list: (list) list of dicts to be stored,
        filepath: (str) path to the output file. If suffix .jsonl is not given then methods appends
        .jsonl suffix into the file.
    """
    jsonl_suffix = ".jsonl"
    if not filepath.endswith(jsonl_suffix):
        filepath = filepath + jsonl_suffix
    with tentaclio.open(filepath, "w") as out:
        for ddict in data_list:
            json_out = json.dumps(ddict) + "\n"
            out.write(json_out)
