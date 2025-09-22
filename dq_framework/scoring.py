from typing import List, Dict

class DQScoreCalculator:
    def __init__(self, global_settings):
        self.global_settings = global_settings

    def calculate(self, rule_results: List[Dict], rule_type: str = None) -> float:
        total_weight = 0.0
        weighted_score = 0.0
        for r in rule_results:
            if rule_type and r.get('classification') != rule_type:
                continue
            weight = r.get('weight', 1.0)
            criticality = r.get('criticality', 'medium').lower()
            crit_mult = {'critical': 3.0, 'high': 2.0, 'medium': 1.0}.get(criticality, 1.0)
            weight *= crit_mult
            classification = r.get('classification', 'technical').lower()
            if classification == "business":
                weight *= self.global_settings.get('business_weight', 2.0)
            else:
                weight *= self.global_settings.get('technical_weight', 1.0)
            passed_score = 100.0 if r.get('success') else 0.0
            weighted_score += passed_score * weight
            total_weight += weight
        return weighted_score / total_weight if total_weight else 0.0
