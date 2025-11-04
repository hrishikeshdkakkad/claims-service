"""
CSV to JSON converter for claim data.
Handles the inconsistent field naming in CSV files and converts to clean API format.
"""
import csv
from typing import List, Dict, Any
from pathlib import Path
import json

from claim_process.services.field_mapper import get_field_mapper


def convert_csv_to_api_format(csv_path: str) -> Dict[str, Any]:
    """
    Convert CSV file with inconsistent field names to clean API format.

    Args:
        csv_path: Path to CSV file (like claim_1234.csv)

    Returns:
        Clean JSON format ready for API
    """
    mapper = get_field_mapper()
    lines = []

    with open(csv_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            # Normalize the messy CSV fields
            normalized = mapper.normalize_record(row)

            # Convert to clean API format
            clean_line = {
                "service_date": normalized.get("service_date"),
                "submitted_procedure": normalized.get("submitted_procedure"),
                "quadrant": normalized.get("quadrant"),
                "plan_group_number": normalized.get("plan_group_number"),
                "subscriber_number": normalized.get("subscriber_number"),
                "provider_npi": normalized.get("provider_npi"),
                "provider_fees": str(normalized.get("provider_fees", "0.00")),
                "allowed_fees": str(normalized.get("allowed_fees", "0.00")),
                "member_coinsurance": str(normalized.get("member_coinsurance", "0.00")),
                "member_copay": str(normalized.get("member_copay", "0.00"))
            }
            lines.append(clean_line)

    # Extract claim ID from filename (e.g., claim_1234.csv -> claim_1234)
    claim_id = Path(csv_path).stem

    return {
        "external_claim_id": claim_id,
        "lines": lines
    }


def main():
    """Example usage: Convert the sample CSV to clean API format."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m claim_process.csv_converter <csv_file>")
        print("\nExample:")
        print("  python -m claim_process.csv_converter claim_1234.csv")
        return

    csv_file = sys.argv[1]

    try:
        # Convert CSV to API format
        api_payload = convert_csv_to_api_format(csv_file)

        # Pretty print the result
        print("Clean API Payload:")
        print(json.dumps(api_payload, indent=2))

        # Save to file
        output_file = f"{Path(csv_file).stem}_api.json"
        with open(output_file, 'w') as f:
            json.dump(api_payload, f, indent=2)

        print(f"\nSaved to: {output_file}")

    except Exception as e:
        print(f"Error converting CSV: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()