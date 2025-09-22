from pyspark.sql import SparkSession

class PartitionValidator:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_partitions(self, table: str):
        partitions = []
        try:
            descr = self.spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
            for row in descr:
                if 'Partition' in (row.col_name or ''):
                    info = row.data_type
                    partitions.extend(
                        p.strip() for p in info.strip('()').split(',') if p.strip()
                    )
            return partitions
        except Exception:
            return []

    def enforce_partition_filter(self, partitions: list, filter_clause: str, force_run: bool):
        if not filter_clause:
            if force_run:
                return
            else:
                raise ValueError("Partition filter required unless force_run=True")
        lower_filter = filter_clause.lower()
        partition_cols_lower = [p.lower() for p in partitions]
        if not any(p in lower_filter for p in partition_cols_lower):
            if not force_run:
                raise ValueError(f"Partition filter must include at least one partition column: {partitions}")
