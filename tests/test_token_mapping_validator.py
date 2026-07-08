from poly_alpha_sniper.core.contracts import RejectReason
from poly_alpha_sniper.deterministic_intelligence.token_mapping_validator import (
    validate_token_mapping)
from poly_alpha_sniper.discovery.threshold_parser import parse_market_text

PARSED_UPDOWN = parse_market_text("Bitcoin Up or Down - July 8, 3:40 PM ET")
PARSED_THRESH = parse_market_text("Will BTC be above $108,500 at 3:45 PM ET?")


def test_up_down_order_up_first():
    raw = {"outcomes": ["Up", "Down"], "clobTokenIds": ["tok_up", "tok_down"]}
    r = validate_token_mapping(raw, PARSED_UPDOWN)
    assert r.ok
    assert r.direction_up_means_yes is True
    assert r.confidence >= 95


def test_up_down_order_down_first():
    raw = {"outcomes": ["Down", "Up"], "clobTokenIds": ["tok_down", "tok_up"]}
    r = validate_token_mapping(raw, PARSED_UPDOWN)
    assert r.ok
    assert r.direction_up_means_yes is False


def test_yes_no_labels():
    raw = {"outcomes": ["Yes", "No"], "clobTokenIds": ["t1", "t2"]}
    r = validate_token_mapping(raw, PARSED_THRESH)
    assert r.ok
    assert r.direction_up_means_yes is True


def test_json_string_outcomes():
    raw = {"outcomes": '["Up", "Down"]', "clobTokenIds": '["a", "b"]'}
    r = validate_token_mapping(raw, PARSED_UPDOWN)
    assert r.ok


def test_ambiguous_labels_rejected():
    raw = {"outcomes": ["Moon", "Dust"], "clobTokenIds": ["t1", "t2"]}
    r = validate_token_mapping(raw, PARSED_UPDOWN)
    assert not r.ok
    assert r.reject_reason == RejectReason.TOKEN_MAPPING_UNCLEAR


def test_three_outcomes_rejected():
    raw = {"outcomes": ["Up", "Down", "Flat"], "clobTokenIds": ["a", "b", "c"]}
    r = validate_token_mapping(raw, PARSED_UPDOWN)
    assert not r.ok


def test_missing_tokens_rejected():
    raw = {"outcomes": ["Up", "Down"], "clobTokenIds": []}
    r = validate_token_mapping(raw, PARSED_UPDOWN)
    assert not r.ok
