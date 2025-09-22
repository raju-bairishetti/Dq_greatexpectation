import pytest
from dq_framework.partition import PartitionValidator

class DummySpark:
    def sql(self, query):
        return [
            type("Row", (), {"col_name": "dt_partition", "data_type": "(dt_partition)"}),
        ]

def test_partition_enforcement_fail():
    validator = PartitionValidator(DummySpark())
    part_cols = validator.get_partitions("dummy_table")
    with pytest.raises(ValueError):
        validator.enforce_partition_filter(part_cols, "non_partition_col = '2025-01-01'", False)

def test_partition_enforcement_pass():
    validator = PartitionValidator(DummySpark())
    part_cols = validator.get_partitions("dummy_table")
    validator.enforce_partition_filter(part_cols, "dt_partition = '2025-01-01'", False)
