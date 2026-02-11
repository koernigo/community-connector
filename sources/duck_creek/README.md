# Duck Creek XML Connector

This connector reads Duck Creek policy XML files from a file location (local filesystem or Unity Catalog volume) and extracts data into normalized Unity Catalog tables.

## Overview

Duck Creek is a leading provider of software for the property and casualty (P&C) insurance industry. This connector parses Duck Creek's XML policy export format and transforms the nested XML structure into relational tables suitable for analytics.

## Configuration Options

| Option | Required | Description |
|--------|----------|-------------|
| `file_path` | Yes | Path to the directory containing XML files. Can be a local path or Unity Catalog volume path (e.g., `/Volumes/catalog/schema/volume/`) |
| `file_pattern` | No | Glob pattern for XML files. Default: `*.xml` |

## Supported Tables

The connector extracts the following tables from Duck Creek XML files:

| Table | Description |
|-------|-------------|
| `policies` | Core policy information including effective dates, premiums, and status |
| `lines` | Insurance lines associated with each policy |
| `coverages` | Coverage details within each line |
| `exposures` | Exposure data for rating purposes |
| `limits` | Limit amounts and types |
| `deductibles` | Deductible amounts and types |
| `locations` | Physical locations covered by the policy |
| `buildings` | Building information at each location |
| `occupancies` | Occupancy details for each location |
| `accounts` | Account holder information |
| `addresses` | Address details for accounts and locations |
| `underwriter_referrals` | Underwriting referral records |
| `tax_surcharges` | Tax and surcharge details by state |

## Table Relationships

```
policies
├── lines
│   ├── coverages
│   ├── exposures
│   ├── limits
│   ├── deductibles
│   └── tax_surcharges (line-level)
├── tax_surcharges (policy-level)
├── underwriter_referrals
└── accounts
    ├── addresses
    └── locations
        ├── addresses
        ├── buildings
        └── occupancies
```

## Usage

### Local Development / Testing

```python
from sources.duck_creek.duck_creek import LakeflowConnect

# Initialize connector
connector = LakeflowConnect({
    "file_path": "/path/to/xml/files",
    "file_pattern": "*.xml"
})

# List available tables
tables = connector.list_tables()

# Read data from a table
records, offset = connector.read_table("policies", {}, {})
for record in records:
    print(record)
```

### Unity Catalog Volume Path

When running in Databricks, you can point the connector to a Unity Catalog volume:

```python
connector = LakeflowConnect({
    "file_path": "/Volumes/my_catalog/my_schema/duck_creek_files",
    "file_pattern": "Policy*.xml"
})
```

## Incremental Sync

The connector supports incremental sync based on file modification time. When reading a table:

1. The connector tracks the maximum file modification time from the last read
2. On subsequent reads, only files modified after this time are processed
3. The offset is stored and passed back to enable incremental processing

## Schema Details

### policies

| Column | Type | Description |
|--------|------|-------------|
| policy_id | string | Unique policy identifier |
| session_id | string | Session identifier |
| effective_date | string | Policy effective date |
| expiration_date | string | Policy expiration date |
| term | string | Policy term in months |
| line_of_business | string | Line of business (e.g., BusinessOwners) |
| quote_number | string | Quote number |
| policy_number | string | Policy number |
| status | string | Policy status |
| premium | double | Total premium amount |
| ... | ... | Additional fields |

### coverages

| Column | Type | Description |
|--------|------|-------------|
| coverage_id | string | Unique coverage identifier |
| line_id | string | Parent line identifier |
| policy_id | string | Parent policy identifier |
| type | string | Coverage type |
| premium | double | Coverage premium |
| is_cov_endorsement | string | Endorsement flag |

## Running Tests

```bash
pytest sources/duck_creek/test/test_duck_creek_lakeflow_connect.py -v
```

## Notes

- XML files are parsed using Python's built-in `xml.etree.ElementTree`
- The connector handles nested XML structures and flattens them into relational tables
- Foreign key relationships are maintained through `policy_id`, `line_id`, `location_id`, etc.
- All records include `source_file` and `_extracted_at` metadata fields for lineage tracking
