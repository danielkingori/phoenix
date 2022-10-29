"""Unit tests for prodigy_utils."""
import pytest

from phoenix.tag.labelling import prodigy_utils


def test_span_to_text_snippets():
    """Test annotation extraction from span_json."""
    test_json = {
        "text": "This is a keyword for a label. These key words are another.",
        "spans": [
            {"start": 10, "end": 17, "label": "test_label"},
            {"start": 37, "end": 46, "label": "test_label"},
        ],
        "extra_key_1": "extra_data_1",
    }

    expected_snippets = [
        {"label": "test_label", "text": "keyword"},
        {"label": "test_label", "text": "key words"},
    ]

    actual_snippets = prodigy_utils.span_to_text_snippets(test_json)
    assert [i for i in expected_snippets if i not in actual_snippets] == []


def test_span_to_text_snippets_except_spans():
    """Test that not having a spans key raises an exception."""
    test_json = {
        "text": "This is a keyword for a label. These key words are another.",
        "not_a_spans": [
            {"start": 10, "end": 17, "label": "test_label"},
            {"start": 37, "end": 46, "label": "test_label"},
        ],
        "extra_key_1": "extra_data_1",
    }

    with pytest.raises(Exception):
        _ = prodigy_utils.span_to_text_snippets(test_json)


def test_span_to_text_snippets_except_text():
    """Test that not having a text key raises an exception."""
    test_json = {
        "not_a_text": "This is a keyword for a label. These key words are another.",
        "spans": [
            {"start": 10, "end": 17, "label": "test_label"},
            {"start": 37, "end": 46, "label": "test_label"},
        ],
        "extra_key_1": "extra_data_1",
    }

    with pytest.raises(Exception):
        _ = prodigy_utils.span_to_text_snippets(test_json)


@pytest.mark.parametrize(
    "label, text, expected_pattern",
    [
        (
            "test_label",
            "hello world",
            {"label": "test_label", "pattern": [{"lower": "hello"}, {"lower": "world"}]},
        ),
        (
            "test_label",
            "Min nizanibû a",
            {
                "label": "test_label",
                "pattern": [{"lower": "Min"}, {"lower": "nizanibû"}, {"lower": "a"}],
            },
        ),
        (
            "test_label",
            "لە ســـاڵەکانی ١٩٥٠دا یان",
            {
                "label": "test_label",
                "pattern": [
                    {"lower": "لە"},
                    {"lower": "ســـاڵەکانی"},
                    {"lower": "١٩٥٠دا"},
                    {"lower": "یان"},
                ],
            },
        ),
        (
            "test_label",
            """ مبادرة #البابا_فرنسيس في يوم #لبنان """,
            {
                "label": "test_label",
                "pattern": [
                    {"lower": "مبادرة"},
                    {"lower": "#البابا_فرنسيس"},
                    {"lower": "في"},
                    {"lower": "يوم"},
                    {"lower": "#لبنان"},
                ],
            },
        ),
    ],
)
def test_create_spacy_pattern_json(label, text, expected_pattern):
    """Test create_spacy_pattern_json works for roman and arabic scripts."""
    actual_pattern = prodigy_utils.create_spacy_pattern_json(label, text)
    assert actual_pattern == expected_pattern


def test_text_snippet_to_patterns():
    """Test text_snippet_to_patterns returns a patterns list."""
    input_snippets = [
        {"label": "test_label", "text": "keyword"},
        {"label": "test_label", "text": "key words"},
    ]
    label = "pizza"

    expected_patterns_list = [
        {"label": "pizza", "pattern": [{"lower": "keyword"}]},
        {"label": "pizza", "pattern": [{"lower": "key"}, {"lower": "words"}]},
    ]

    actual_patterns_list = prodigy_utils.text_snippet_to_patterns(label, input_snippets)

    assert actual_patterns_list == expected_patterns_list


def test_text_snippet_to_patterns_duplicates():
    """Test text_snippet_to_patterns returns a deduplicated patterns list."""
    input_snippets = [
        {"label": "test_label", "text": "keyword"},
        {"label": "test_label", "text": "key words"},
        {"label": "test_label", "text": "key words"},
    ]
    label = "pizza"

    expected_patterns_list = [
        {"label": "pizza", "pattern": [{"lower": "keyword"}]},
        {"label": "pizza", "pattern": [{"lower": "key"}, {"lower": "words"}]},
    ]

    actual_patterns_list = prodigy_utils.text_snippet_to_patterns(label, input_snippets)

    assert actual_patterns_list == expected_patterns_list
