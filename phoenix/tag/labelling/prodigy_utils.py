"""Utility functions for prodigy text annotation."""
from typing import Any, Dict, List


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
