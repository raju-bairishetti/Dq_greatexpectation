from dq_framework.config import load_configs, get_table_config
from dq_framework.partition import PartitionValidator
from dq_framework.executor import RuleExecutor
from dq_framework.scoring import DQScoreCalculator
from dq_framework.audit import AuditManager
from pyspark.sql import SparkSession
from typing import Optional

class DataQualityFramework:

    def __init__(self, spark: Optional[SparkSession], app_config_path: str, dq_config_path: str):
        self.spark = spark or SparkSession.builder.appName("DQFramework").getOrCreate()
        self.configs = load_configs(app_config_path, dq_config_path)
        self.validator = PartitionValidator(self.spark)
        self.executor = RuleExecutor(self.spark)
        self.scorer = DQScoreCalculator(self.configs["dq_config"].get("global_settings", {}))
        self.audit = AuditManager(self.spark, self.configs["dq_config"].get("audit_config", {}))

    def run_validation(self, table_name: str, partition_filter: str, rule_type: Optional[str] = None, force_run=False, batch_id=None):
        partitions = self.validator.get_partitions(table_name)
        self.validator.enforce_partition_filter(partitions, partition_filter, force_run)
        df = self.spark.sql(f"SELECT * FROM {table_name} WHERE {partition_filter}")

        table_config = get_table_config(self.configs["dq_config"], table_name)
        rules = []
        # Merge table and column rules filtering by rule_type
        for rule in table_config.get('table_expectations', []):
            if rule_type and rule.get('classification') != rule_type:
                continue
            rules.append(rule)
        for col in table_config.get('column_expectations', []):
            for rule in col.get('expectations', []):
                if rule_type and rule.get('classification') != rule_type:
                    continue
                rule['column'] = col['column_name']
                rules.append(rule)

        results = self.executor.run(df, rules, rule_type)
        score = self.scorer.calculate(results, rule_type)
        audit_result = self.audit.save(table_name, partition_filter, rule_type, score, results, batch_id)
        return {
            "dq_score": score,
            "results": results,
            "audit": audit_result
        }
