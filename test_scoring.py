import pytest
from dq_framework.scoring import DQScoreCalculator

def test_weighted_score():
    rules = [
        {"success": True, "classification": "technical", "weight": 1.0, "criticality": "critical"},
        {"success": False, "classification": "business", "weight": 2.0, "criticality": "high"},
        {"success": True, "classification": "business", "weight": 3.0, "criticality": "medium"},
    ]
    settings = {"business_weight": 2.0, "technical_weight": 1.0}
    calc = DQScoreCalculator(settings)
    score = calc.calculate(rules)
    assert 0 < score < 100
