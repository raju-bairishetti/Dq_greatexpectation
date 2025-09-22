import yaml
import json
from typing import Dict

def load_configs(app_config_path: str, dq_config_path: str) -> Dict:
    """Load and validate application and DQ config files (JSON + YAML)."""
    with open(app_config_path, 'r') as f:
        app_config = json.load(f)
    with open(dq_config_path, 'r') as f:
        dq_config = yaml.safe_load(f)
    # Add validations here if needed
    return {"app_config": app_config, "dq_config": dq_config}

def get_table_config(dq_config: Dict, table_name: str) -> Dict:
    for table in dq_config.get('tables', []):
        if table['table_name'] == table_name:
            return table
    raise ValueError(f"No config for table '{table_name}'")
