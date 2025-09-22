from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from typing import List, Dict

class RuleExecutor:
    def __init__(self, spark_session):
        self.spark = spark_session

    def run(self, df, rules: List[Dict], filter_rule_type: str = None) -> List[Dict]:
        gedf = SparkDFDataset(df)
        results = []
        for rule in rules:
            classification = rule.get('classification', '')
            if filter_rule_type and classification != filter_rule_type:
                continue
            exp_name = rule['expectation_name']
            params = rule.get('params', {})
            try:
                if 'column' in rule:
                    params['column'] = rule['column']
                res = getattr(gedf, exp_name)(**params)
                results.append({
                    'expectation_name': exp_name,
                    'column': rule.get('column'),
                    'classification': classification,
                    'success': res['success'],
                    'criticality': rule.get('criticality', 'medium'),
                    'weight': rule.get('weight', 1.0),
                    'description': rule.get('description', '')
                })
            except Exception as e:
                results.append({
                    'expectation_name': exp_name,
                    'column': rule.get('column'),
                    'classification': classification,
                    'success': False,
                    'criticality': rule.get('criticality', 'medium'),
                    'weight': rule.get('weight', 1.0),
                    'description': rule.get('description', ''),
                    'error': str(e)
                })
        return results
