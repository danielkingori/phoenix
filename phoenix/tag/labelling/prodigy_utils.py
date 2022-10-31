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
