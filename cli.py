import argparse
import sys
from dq_framework.framework import DataQualityFramework

def main():
    parser = argparse.ArgumentParser(description="Data Quality Framework CLI")
    parser.add_argument("--app_config", default="app_config.json", help="Path to app config")
    parser.add_argument("--dq_config", default="dq_config.yaml", help="Path to dq config")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run DQ validation")
    run_parser.add_argument("--table", required=True)
    run_parser.add_argument("--partition_filter", required=True)
    run_parser.add_argument("--rule_type", choices=["technical", "business"])
    run_parser.add_argument("--force_run", action="store_true")

    args = parser.parse_args()

    if args.command == "run":
        dq = DataQualityFramework(None, args.app_config, args.dq_config)
        result = dq.run_validation(args.table, args.partition_filter, args.rule_type, args.force_run)
        print(f"DQ Score: {result['dq_score']}")
        print("Pass:", "Yes" if result['dq_score'] >= 90 else "No")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
