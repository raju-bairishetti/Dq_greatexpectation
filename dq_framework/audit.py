import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging
from datetime import datetime

class AuditManager:
    def __init__(self, spark: SparkSession, audit_config):
        self.spark = spark
        self.config = audit_config
        self.backend = audit_config.get('backend_type', 'iceberg')
        self.audit_table = audit_config.get('audit_table')
        self._initialize()

    def _initialize(self):
        schema = StructType([
            StructField("report_id", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("partition_filter", StringType()),
            StructField("rule_type", StringType()),
            StructField("execution_date", StringType(), False),
            StructField("execution_timestamp", TimestampType(), False),
            StructField("batch_id", StringType()),
            StructField("total_rules", IntegerType(), False),
            StructField("passed_rules", IntegerType(), False),
            StructField("failed_rules", IntegerType(), False),
            StructField("dq_score", DoubleType(), False),
            StructField("threshold", DoubleType(), False),
            StructField("is_pass", BooleanType(), False),
            StructField("rule_results", StringType(), False),
        ])
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.audit_table}")
        except Exception:
            parts = ", ".join(self.config.get('partition_columns', ['execution_date']))
            ddl = ",\n  ".join(f"{f.name} {f.dataType.simpleString().upper()}" for f in schema.fields)
            self.spark.sql(f"""
                CREATE TABLE {self.audit_table} (
                    {ddl}
                ) USING ICEBERG
                PARTITIONED BY ({parts})
            """)
            logging.info(f"Created audit table {self.audit_table}")

    def save(self, table_name, partition_filter, rule_type, dq_score, results, batch_id=None):
        now = datetime.now()
        total = len(results)
        passed = sum(1 for r in results if r.get('success'))
        failed = total - passed
        report_id = f"{table_name}_{batch_id or now.strftime('%Y%m%d%H%M%S')}"

        df = self.spark.createDataFrame([{
            'report_id': report_id,
            'table_name': table_name,
            'partition_filter': partition_filter,
            'rule_type': rule_type,
            'execution_date': now.strftime('%Y-%m-%d'),
            'execution_timestamp': now,
            'batch_id': batch_id,
            'total_rules': total,
            'passed_rules': passed,
            'failed_rules': failed,
            'dq_score': dq_score,
            'threshold': 0,
            'is_pass': dq_score >= 0,
            'rule_results': json.dumps(results)
        }])

        if self.backend == 'iceberg':
            df.writeTo(self.audit_table).append()
        else:
            # Implement other backends like Postgres as needed
            pass
        return {
            'report_id': report_id,
            'passed': passed,
            'failed': failed,
            'dq_score': dq_score,
        }
