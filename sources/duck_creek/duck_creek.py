import os
import glob
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Iterator, Any, Optional, Tuple
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    ArrayType,
    MapType,
)


class LakeflowConnect:
    """
    Duck Creek XML file connector.

    Reads XML policy files from a specified directory and extracts
    data into normalized tables for ingestion into Unity Catalog.
    """

    def __init__(self, options: dict) -> None:
        """
        Initialize the Duck Creek connector.

        Args:
            options: Configuration options including:
                - file_path: Path to directory containing XML files
                  (can be local path or Unity Catalog volume path like /Volumes/catalog/schema/volume/)
                - file_pattern: Optional glob pattern for XML files (default: "*.xml")
        """
        self.file_path = options.get("file_path", "")
        self.file_pattern = options.get("file_pattern", "*.xml")

        if not self.file_path:
            raise ValueError("file_path option is required")

    def list_tables(self) -> List[str]:
        """
        Return the list of available Duck Creek tables.

        Tables are normalized from the nested XML structure into
        relational tables with foreign key relationships.
        """
        return [
            "policies",
            "lines",
            "coverages",
            "exposures",
            "limits",
            "deductibles",
            "locations",
            "buildings",
            "occupancies",
            "accounts",
            "addresses",
            "underwriter_referrals",
            "tax_surcharges",
        ]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Return the Spark schema for a given table.
        """
        schemas = {
            "policies": StructType([
                StructField("policy_id", StringType(), False),
                StructField("session_id", StringType(), True),
                StructField("effective_date", StringType(), True),
                StructField("expiration_date", StringType(), True),
                StructField("term", StringType(), True),
                StructField("line_of_business", StringType(), True),
                StructField("quote_number", StringType(), True),
                StructField("policy_number", StringType(), True),
                StructField("status", StringType(), True),
                StructField("last_transaction_type", StringType(), True),
                StructField("primary_rating_state", StringType(), True),
                StructField("segment", StringType(), True),
                StructField("product", StringType(), True),
                StructField("writing_company", StringType(), True),
                StructField("filing_type", StringType(), True),
                StructField("agency_id", StringType(), True),
                StructField("producer", StringType(), True),
                StructField("premium", DoubleType(), True),
                StructField("premium_written", DoubleType(), True),
                StructField("premium_change", DoubleType(), True),
                StructField("transaction_date", StringType(), True),
                StructField("transaction_sequence_number", StringType(), True),
                StructField("endorsement_number", StringType(), True),
                StructField("term_number", StringType(), True),
                StructField("renewal_type", StringType(), True),
                StructField("cancellation_date", StringType(), True),
                StructField("include_terrorism", StringType(), True),
                StructField("include_gl", StringType(), True),
                StructField("include_property", StringType(), True),
                StructField("source_file", StringType(), True),
                StructField("file_modified_time", StringType(), True),
                StructField("_extracted_at", StringType(), False),
            ]),

            "lines": StructType([
                StructField("line_id", StringType(), False),
                StructField("policy_id", StringType(), False),
                StructField("type", StringType(), True),
                StructField("written", DoubleType(), True),
                StructField("change", DoubleType(), True),
                StructField("is_reportable", StringType(), True),
                StructField("is_final_report", StringType(), True),
                StructField("has_earthquake_been_selected", StringType(), True),
                StructField("source_file", StringType(), True),
                StructField("_extracted_at", StringType(), False),
            ]),

            "coverages": StructType([
                StructField("coverage_id", StringType(), False),
                StructField("line_id", StringType(), False),
                StructField("policy_id", StringType(), False),
                StructField("type", StringType(), True),
                StructField("premium", DoubleType(), True),
                StructField("written", DoubleType(), True),
                StructField("change", DoubleType(), True),
                StructField("is_cov_endorsement", StringType(), True),
                StructField("source_file", StringType(), True),
                StructField("_extracted_at", StringType(), False),
            ]),

            "exposures": StructType([
                StructField("exposure_id", StringType(), False),
                StructField("line_id", StringType(), False),
                StructField("policy_id", StringType(), False),
                StructField("type", StringType(), True),
                StructField("i_value", StringType(), True),
                StructField("f_value", DoubleType(), True),
                StructField("s_value", StringType(), True),
                StructField("source_file", StringType(), True),
                StructField("_extracted_at", StringType(), False),
            ]),

            "limits": StructType([
                StructField("limit_id", StringType(), False),
                StructField("line_id", StringType(), False),
                StructField("policy_id", StringType(), False),
                StructField("type", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("i_value", StringType(), True),
                StructField("f_value", DoubleType(), True),
                StructField("scope", StringType(), True),
                StructField("source_file", StringType(), True),
                StructField("_extracted_at", StringType(), False),
            ]),

            "deductibles": StructType([
                StructField("deductible_id", StringType(), False),
                StructField("line_id", StringType(), False),
                StructField("policy_id", StringType(), False),
                StructField("type", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("i_value", StringType(), True),
                StructField("f_value", DoubleType(), True),
                StructField("scope", StringType(), True),
                StructField("source_file", StringType(), True),
                StructField("_extracted_at", StringType(), False),
            ]),

            "locations": StructType([
                StructField("location_id", StringType(), False),
                StructField("account_id", StringType(), False),
                StructField("policy_id", StringType(), False),
                StructField("deleted", StringType(), True),
                StructField("source_file", StringType(), True),
                StructField("_extracted_at", StringType(), False),
            ]),

            "buildings": StructType([
                StructField("building_id", StringType(), False),
                StructField("location_id", StringType(), False),
                StructField("policy_id", StringType(), False),
                StructField("source_file", StringType(), True),
                StructField("_extracted_at", StringType(), False),
            ]),

            "occupancies": StructType([
                StructField("occupancy_id", StringType(), False),
                StructField("location_id", StringType(), False),
                StructField("policy_id", StringType(), False),
                StructField("source_file", StringType(), True),
                StructField("_extracted_at", StringType(), False),
            ]),

            "accounts": StructType([
                StructField("account_id", StringType(), False),
                StructField("policy_id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("dba", StringType(), True),
                StructField("entity_type", StringType(), True),
                StructField("brief_description", StringType(), True),
                StructField("primary_phone", StringType(), True),
                StructField("fax", StringType(), True),
                StructField("email", StringType(), True),
                StructField("same_as_mailing_address", StringType(), True),
                StructField("sic_code", StringType(), True),
                StructField("source_file", StringType(), True),
                StructField("_extracted_at", StringType(), False),
            ]),

            "addresses": StructType([
                StructField("address_id", StringType(), False),
                StructField("location_id", StringType(), True),
                StructField("account_id", StringType(), True),
                StructField("policy_id", StringType(), False),
                StructField("latitude", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("last_verified", StringType(), True),
                StructField("source_file", StringType(), True),
                StructField("_extracted_at", StringType(), False),
            ]),

            "underwriter_referrals": StructType([
                StructField("referral_id", StringType(), False),
                StructField("policy_id", StringType(), False),
                StructField("source_file", StringType(), True),
                StructField("_extracted_at", StringType(), False),
            ]),

            "tax_surcharges": StructType([
                StructField("tax_surcharge_id", StringType(), False),
                StructField("policy_id", StringType(), False),
                StructField("line_id", StringType(), True),
                StructField("tax_state", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("written", DoubleType(), True),
                StructField("change", DoubleType(), True),
                StructField("type", StringType(), True),
                StructField("source_file", StringType(), True),
                StructField("_extracted_at", StringType(), False),
            ]),
        }

        if table_name not in schemas:
            raise ValueError(f"Unknown table: {table_name}")

        return schemas[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Return metadata for a table including primary keys and ingestion type.
        """
        metadata = {
            "policies": {
                "primary_keys": ["policy_id"],
                "cursor_field": "file_modified_time",
                "ingestion_type": "cdc",
            },
            "lines": {
                "primary_keys": ["line_id", "policy_id"],
                "cursor_field": "_extracted_at",
                "ingestion_type": "cdc",
            },
            "coverages": {
                "primary_keys": ["coverage_id", "line_id", "policy_id"],
                "cursor_field": "_extracted_at",
                "ingestion_type": "cdc",
            },
            "exposures": {
                "primary_keys": ["exposure_id", "line_id", "policy_id"],
                "cursor_field": "_extracted_at",
                "ingestion_type": "cdc",
            },
            "limits": {
                "primary_keys": ["limit_id", "line_id", "policy_id"],
                "cursor_field": "_extracted_at",
                "ingestion_type": "cdc",
            },
            "deductibles": {
                "primary_keys": ["deductible_id", "line_id", "policy_id"],
                "cursor_field": "_extracted_at",
                "ingestion_type": "cdc",
            },
            "locations": {
                "primary_keys": ["location_id", "policy_id"],
                "cursor_field": "_extracted_at",
                "ingestion_type": "cdc",
            },
            "buildings": {
                "primary_keys": ["building_id", "location_id", "policy_id"],
                "cursor_field": "_extracted_at",
                "ingestion_type": "cdc",
            },
            "occupancies": {
                "primary_keys": ["occupancy_id", "location_id", "policy_id"],
                "cursor_field": "_extracted_at",
                "ingestion_type": "cdc",
            },
            "accounts": {
                "primary_keys": ["account_id", "policy_id"],
                "cursor_field": "_extracted_at",
                "ingestion_type": "cdc",
            },
            "addresses": {
                "primary_keys": ["address_id", "policy_id"],
                "cursor_field": "_extracted_at",
                "ingestion_type": "cdc",
            },
            "underwriter_referrals": {
                "primary_keys": ["referral_id", "policy_id"],
                "cursor_field": "_extracted_at",
                "ingestion_type": "cdc",
            },
            "tax_surcharges": {
                "primary_keys": ["tax_surcharge_id", "policy_id"],
                "cursor_field": "_extracted_at",
                "ingestion_type": "cdc",
            },
        }

        if table_name not in metadata:
            raise ValueError(f"Unknown table: {table_name}")

        return metadata[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read records from a table.

        For file-based connectors, this reads all XML files and extracts
        the relevant table data. Incremental sync is based on file
        modification time.

        Args:
            table_name: Name of the table to read
            start_offset: Offset containing last_modified_time for incremental reads
            table_options: Additional options

        Returns:
            Tuple of (record iterator, new offset)
        """
        last_modified_time = start_offset.get("last_modified_time", "")

        # Get list of XML files
        xml_files = self._get_xml_files()

        if not xml_files:
            return iter([]), {"last_modified_time": last_modified_time}

        # Filter files based on modification time for incremental sync
        files_to_process = []
        max_modified_time = last_modified_time

        for file_path in xml_files:
            file_mtime = self._get_file_modified_time(file_path)
            if not last_modified_time or file_mtime > last_modified_time:
                files_to_process.append((file_path, file_mtime))
                if file_mtime > max_modified_time:
                    max_modified_time = file_mtime

        # Parse files and extract records
        records = self._extract_table_records(table_name, files_to_process)

        new_offset = {"last_modified_time": max_modified_time}

        return iter(records), new_offset

    def _get_xml_files(self) -> List[str]:
        """Get list of XML files matching the pattern in the configured path."""
        pattern = os.path.join(self.file_path, self.file_pattern)
        return glob.glob(pattern)

    def _get_file_modified_time(self, file_path: str) -> str:
        """Get file modification time as ISO string."""
        mtime = os.path.getmtime(file_path)
        return datetime.fromtimestamp(mtime).isoformat()

    def _extract_table_records(
        self, table_name: str, files: List[Tuple[str, str]]
    ) -> List[dict]:
        """
        Extract records for a specific table from XML files.

        Args:
            table_name: Name of the table to extract
            files: List of (file_path, modified_time) tuples

        Returns:
            List of record dictionaries
        """
        records = []
        extracted_at = datetime.utcnow().isoformat()

        for file_path, file_mtime in files:
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()

                source_file = os.path.basename(file_path)

                # Extract based on table name
                if table_name == "policies":
                    records.extend(self._extract_policies(root, source_file, file_mtime, extracted_at))
                elif table_name == "lines":
                    records.extend(self._extract_lines(root, source_file, extracted_at))
                elif table_name == "coverages":
                    records.extend(self._extract_coverages(root, source_file, extracted_at))
                elif table_name == "exposures":
                    records.extend(self._extract_exposures(root, source_file, extracted_at))
                elif table_name == "limits":
                    records.extend(self._extract_limits(root, source_file, extracted_at))
                elif table_name == "deductibles":
                    records.extend(self._extract_deductibles(root, source_file, extracted_at))
                elif table_name == "locations":
                    records.extend(self._extract_locations(root, source_file, extracted_at))
                elif table_name == "buildings":
                    records.extend(self._extract_buildings(root, source_file, extracted_at))
                elif table_name == "occupancies":
                    records.extend(self._extract_occupancies(root, source_file, extracted_at))
                elif table_name == "accounts":
                    records.extend(self._extract_accounts(root, source_file, extracted_at))
                elif table_name == "addresses":
                    records.extend(self._extract_addresses(root, source_file, extracted_at))
                elif table_name == "underwriter_referrals":
                    records.extend(self._extract_underwriter_referrals(root, source_file, extracted_at))
                elif table_name == "tax_surcharges":
                    records.extend(self._extract_tax_surcharges(root, source_file, extracted_at))

            except ET.ParseError as e:
                print(f"Error parsing {file_path}: {e}")
                continue

        return records

    def _get_text(self, elem: Optional[ET.Element], default: str = None) -> Optional[str]:
        """Safely get text from an element."""
        if elem is not None and elem.text:
            return elem.text.strip()
        return default

    def _get_float(self, elem: Optional[ET.Element], default: float = None) -> Optional[float]:
        """Safely get float value from an element."""
        text = self._get_text(elem)
        if text:
            try:
                return float(text)
            except ValueError:
                pass
        return default

    def _extract_policies(
        self, root: ET.Element, source_file: str, file_mtime: str, extracted_at: str
    ) -> List[dict]:
        """Extract policy records from XML."""
        records = []
        session_id = root.get("id")

        for data in root.findall(".//data"):
            for policy in data.findall("policy"):
                policy_id = policy.get("id")

                record = {
                    "policy_id": policy_id,
                    "session_id": session_id,
                    "effective_date": self._get_text(policy.find("EffectiveDate")),
                    "expiration_date": self._get_text(policy.find("ExpirationDate")),
                    "term": self._get_text(policy.find("Term")),
                    "line_of_business": self._get_text(policy.find("LineOfBusiness")),
                    "quote_number": self._get_text(policy.find("QuoteNumber")),
                    "policy_number": self._get_text(policy.find("PolicyNumber")),
                    "status": self._get_text(policy.find("Status")),
                    "last_transaction_type": self._get_text(policy.find("LastTransactionType")),
                    "primary_rating_state": self._get_text(policy.find("PrimaryRatingState")),
                    "segment": self._get_text(policy.find("Segment")),
                    "product": self._get_text(policy.find("Product")),
                    "writing_company": self._get_text(policy.find("WritingCompany")),
                    "filing_type": self._get_text(policy.find("FilingType")),
                    "agency_id": self._get_text(policy.find("AgencyID")),
                    "producer": self._get_text(policy.find("Producer")),
                    "premium": self._get_float(policy.find("Premium")),
                    "premium_written": self._get_float(policy.find("PremiumWritten")),
                    "premium_change": self._get_float(policy.find("PremiumChange")),
                    "transaction_date": self._get_text(policy.find("TransactionDate")),
                    "transaction_sequence_number": self._get_text(policy.find("TransactionSequenceNumber")),
                    "endorsement_number": self._get_text(policy.find("EndorsementNumber")),
                    "term_number": self._get_text(policy.find("TermNumber")),
                    "renewal_type": self._get_text(policy.find("RenewalType")),
                    "cancellation_date": self._get_text(policy.find("CancellationDate")),
                    "include_terrorism": self._get_text(policy.find("IncludeTerrorism")),
                    "include_gl": self._get_text(policy.find("IncludeGL")),
                    "include_property": self._get_text(policy.find("IncludeProperty")),
                    "source_file": source_file,
                    "file_modified_time": file_mtime,
                    "_extracted_at": extracted_at,
                }
                records.append(record)

        return records

    def _extract_lines(
        self, root: ET.Element, source_file: str, extracted_at: str
    ) -> List[dict]:
        """Extract line records from XML."""
        records = []

        for data in root.findall(".//data"):
            for policy in data.findall("policy"):
                policy_id = policy.get("id")

                for line in policy.findall("line"):
                    line_id = line.get("id")

                    record = {
                        "line_id": line_id,
                        "policy_id": policy_id,
                        "type": self._get_text(line.find("Type")),
                        "written": self._get_float(line, line.get("written")),
                        "change": self._get_float(line, line.get("change")),
                        "is_reportable": self._get_text(line.find("IsReportable")),
                        "is_final_report": self._get_text(line.find("IsFinalReport")),
                        "has_earthquake_been_selected": self._get_text(line.find("HasEarthquakeBeenSelected")),
                        "source_file": source_file,
                        "_extracted_at": extracted_at,
                    }
                    records.append(record)

        return records

    def _extract_coverages(
        self, root: ET.Element, source_file: str, extracted_at: str
    ) -> List[dict]:
        """Extract coverage records from XML."""
        records = []
        coverage_counter = 0

        for data in root.findall(".//data"):
            for policy in data.findall("policy"):
                policy_id = policy.get("id")

                for line in policy.findall("line"):
                    line_id = line.get("id")

                    for coverage in line.findall("coverage"):
                        coverage_id = coverage.get("id")
                        if not coverage_id:
                            coverage_counter += 1
                            coverage_id = f"cov_{policy_id}_{line_id}_{coverage_counter}"

                        record = {
                            "coverage_id": coverage_id,
                            "line_id": line_id,
                            "policy_id": policy_id,
                            "type": self._get_text(coverage.find("Type")),
                            "premium": self._get_float(coverage.find("Premium")),
                            "written": self._get_float(coverage, coverage.get("written")),
                            "change": self._get_float(coverage, coverage.get("change")),
                            "is_cov_endorsement": self._get_text(coverage.find("IsCovEndorsement")),
                            "source_file": source_file,
                            "_extracted_at": extracted_at,
                        }
                        records.append(record)

        return records

    def _extract_exposures(
        self, root: ET.Element, source_file: str, extracted_at: str
    ) -> List[dict]:
        """Extract exposure records from XML."""
        records = []
        exposure_counter = 0

        for data in root.findall(".//data"):
            for policy in data.findall("policy"):
                policy_id = policy.get("id")

                for line in policy.findall("line"):
                    line_id = line.get("id")

                    for exposure in line.findall("exposure"):
                        exposure_id = exposure.get("id")
                        if not exposure_id:
                            exposure_counter += 1
                            exposure_id = f"exp_{policy_id}_{line_id}_{exposure_counter}"

                        record = {
                            "exposure_id": exposure_id,
                            "line_id": line_id,
                            "policy_id": policy_id,
                            "type": self._get_text(exposure.find("Type")),
                            "i_value": self._get_text(exposure.find("iValue")),
                            "f_value": self._get_float(exposure.find("fValue")),
                            "s_value": self._get_text(exposure.find("sValue")),
                            "source_file": source_file,
                            "_extracted_at": extracted_at,
                        }
                        records.append(record)

        return records

    def _extract_limits(
        self, root: ET.Element, source_file: str, extracted_at: str
    ) -> List[dict]:
        """Extract limit records from XML."""
        records = []
        limit_counter = 0

        for data in root.findall(".//data"):
            for policy in data.findall("policy"):
                policy_id = policy.get("id")

                for line in policy.findall("line"):
                    line_id = line.get("id")

                    for limit in line.findall("limit"):
                        limit_id = limit.get("id")
                        if not limit_id:
                            limit_counter += 1
                            limit_id = f"lim_{policy_id}_{line_id}_{limit_counter}"

                        record = {
                            "limit_id": limit_id,
                            "line_id": line_id,
                            "policy_id": policy_id,
                            "type": self._get_text(limit.find("Type")),
                            "amount": self._get_float(limit.find("Amount")),
                            "i_value": self._get_text(limit.find("iValue")),
                            "f_value": self._get_float(limit.find("fValue")),
                            "scope": self._get_text(limit.find("Scope")),
                            "source_file": source_file,
                            "_extracted_at": extracted_at,
                        }
                        records.append(record)

        return records

    def _extract_deductibles(
        self, root: ET.Element, source_file: str, extracted_at: str
    ) -> List[dict]:
        """Extract deductible records from XML."""
        records = []
        deductible_counter = 0

        for data in root.findall(".//data"):
            for policy in data.findall("policy"):
                policy_id = policy.get("id")

                for line in policy.findall("line"):
                    line_id = line.get("id")

                    for deductible in line.findall("deductible"):
                        deductible_id = deductible.get("id")
                        if not deductible_id:
                            deductible_counter += 1
                            deductible_id = f"ded_{policy_id}_{line_id}_{deductible_counter}"

                        record = {
                            "deductible_id": deductible_id,
                            "line_id": line_id,
                            "policy_id": policy_id,
                            "type": self._get_text(deductible.find("Type")),
                            "amount": self._get_float(deductible.find("Amount")),
                            "i_value": self._get_text(deductible.find("iValue")),
                            "f_value": self._get_float(deductible.find("fValue")),
                            "scope": self._get_text(deductible.find("Scope")),
                            "source_file": source_file,
                            "_extracted_at": extracted_at,
                        }
                        records.append(record)

        return records

    def _extract_locations(
        self, root: ET.Element, source_file: str, extracted_at: str
    ) -> List[dict]:
        """Extract location records from XML."""
        records = []

        for data in root.findall(".//data"):
            policy = data.find("policy")
            policy_id = policy.get("id") if policy is not None else None

            for account in data.findall(".//account"):
                account_id = account.get("id")

                for location in account.findall("location"):
                    location_id = location.get("id")

                    record = {
                        "location_id": location_id,
                        "account_id": account_id,
                        "policy_id": policy_id,
                        "deleted": location.get("deleted"),
                        "source_file": source_file,
                        "_extracted_at": extracted_at,
                    }
                    records.append(record)

        return records

    def _extract_buildings(
        self, root: ET.Element, source_file: str, extracted_at: str
    ) -> List[dict]:
        """Extract building records from XML."""
        records = []

        for data in root.findall(".//data"):
            policy = data.find("policy")
            policy_id = policy.get("id") if policy is not None else None

            for account in data.findall(".//account"):
                account_id = account.get("id")

                # Buildings can be direct children of account or nested under locations
                # Check direct account children first
                for building in account.findall("building"):
                    building_id = building.get("id")
                    # Try to find associated location
                    location = account.find("location")
                    location_id = location.get("id") if location is not None else None

                    record = {
                        "building_id": building_id,
                        "location_id": location_id,
                        "policy_id": policy_id,
                        "source_file": source_file,
                        "_extracted_at": extracted_at,
                    }
                    records.append(record)

                # Also check buildings under locations
                for location in account.findall("location"):
                    location_id = location.get("id")
                    for building in location.findall("building"):
                        building_id = building.get("id")

                        record = {
                            "building_id": building_id,
                            "location_id": location_id,
                            "policy_id": policy_id,
                            "source_file": source_file,
                            "_extracted_at": extracted_at,
                        }
                        records.append(record)

        return records

    def _extract_occupancies(
        self, root: ET.Element, source_file: str, extracted_at: str
    ) -> List[dict]:
        """Extract occupancy records from XML."""
        records = []

        for data in root.findall(".//data"):
            policy = data.find("policy")
            policy_id = policy.get("id") if policy is not None else None

            for account in data.findall(".//account"):
                account_id = account.get("id")

                # Occupancies can be direct children of account or nested under locations
                # Check direct account children first
                for occupancy in account.findall("occupancy"):
                    occupancy_id = occupancy.get("id")
                    # Try to find associated location
                    location = account.find("location")
                    location_id = location.get("id") if location is not None else None

                    record = {
                        "occupancy_id": occupancy_id,
                        "location_id": location_id,
                        "policy_id": policy_id,
                        "source_file": source_file,
                        "_extracted_at": extracted_at,
                    }
                    records.append(record)

                # Also check occupancies under locations
                for location in account.findall("location"):
                    location_id = location.get("id")
                    for occupancy in location.findall("occupancy"):
                        occupancy_id = occupancy.get("id")

                        record = {
                            "occupancy_id": occupancy_id,
                            "location_id": location_id,
                            "policy_id": policy_id,
                            "source_file": source_file,
                            "_extracted_at": extracted_at,
                        }
                        records.append(record)

        return records

    def _extract_accounts(
        self, root: ET.Element, source_file: str, extracted_at: str
    ) -> List[dict]:
        """Extract account records from XML."""
        records = []

        for data in root.findall(".//data"):
            policy = data.find("policy")
            policy_id = policy.get("id") if policy is not None else None

            for account in data.findall(".//account"):
                account_id = account.get("id")

                record = {
                    "account_id": account_id,
                    "policy_id": policy_id,
                    "name": self._get_text(account.find("Name")),
                    "dba": self._get_text(account.find("DBA")),
                    "entity_type": self._get_text(account.find("EntityType")),
                    "brief_description": self._get_text(account.find("BriefDescription")),
                    "primary_phone": self._get_text(account.find("PrimaryPhone")),
                    "fax": self._get_text(account.find("Fax")),
                    "email": self._get_text(account.find("Email")),
                    "same_as_mailing_address": self._get_text(account.find("SameAsMailingAddress")),
                    "sic_code": self._get_text(account.find("SICCode")),
                    "source_file": source_file,
                    "_extracted_at": extracted_at,
                }
                records.append(record)

        return records

    def _extract_addresses(
        self, root: ET.Element, source_file: str, extracted_at: str
    ) -> List[dict]:
        """Extract address records from XML."""
        records = []

        for data in root.findall(".//data"):
            policy = data.find("policy")
            policy_id = policy.get("id") if policy is not None else None

            for account in data.findall(".//account"):
                account_id = account.get("id")

                # Account-level address
                for address in account.findall("address"):
                    address_id = address.get("id")

                    record = {
                        "address_id": address_id,
                        "location_id": None,
                        "account_id": account_id,
                        "policy_id": policy_id,
                        "latitude": self._get_text(address.find("Latitude")),
                        "longitude": self._get_text(address.find("Longitude")),
                        "last_verified": self._get_text(address.find("LastVerified")),
                        "source_file": source_file,
                        "_extracted_at": extracted_at,
                    }
                    records.append(record)

                # Location-level addresses
                for location in account.findall("location"):
                    location_id = location.get("id")

                    for address in location.findall("address"):
                        address_id = address.get("id")

                        record = {
                            "address_id": address_id,
                            "location_id": location_id,
                            "account_id": account_id,
                            "policy_id": policy_id,
                            "latitude": self._get_text(address.find("Latitude")),
                            "longitude": self._get_text(address.find("Longitude")),
                            "last_verified": self._get_text(address.find("LastVerified")),
                            "source_file": source_file,
                            "_extracted_at": extracted_at,
                        }
                        records.append(record)

        return records

    def _extract_underwriter_referrals(
        self, root: ET.Element, source_file: str, extracted_at: str
    ) -> List[dict]:
        """Extract underwriter referral records from XML."""
        records = []
        referral_counter = 0

        for data in root.findall(".//data"):
            for policy in data.findall("policy"):
                policy_id = policy.get("id")

                for referrals in policy.findall(".//UnderwriterReferrals"):
                    for referral in referrals.findall("UnderwriterReferral"):
                        referral_id = referral.get("id")
                        if not referral_id:
                            referral_counter += 1
                            referral_id = f"ref_{policy_id}_{referral_counter}"

                        record = {
                            "referral_id": referral_id,
                            "policy_id": policy_id,
                            "source_file": source_file,
                            "_extracted_at": extracted_at,
                        }
                        records.append(record)

        return records

    def _extract_tax_surcharges(
        self, root: ET.Element, source_file: str, extracted_at: str
    ) -> List[dict]:
        """Extract tax surcharge records from XML."""
        records = []
        tax_counter = 0

        for data in root.findall(".//data"):
            for policy in data.findall("policy"):
                policy_id = policy.get("id")

                # Policy-level tax surcharges
                for tax in policy.findall("stateTaxSurcharge"):
                    tax_counter += 1
                    tax_id = tax.get("id") or f"tax_{policy_id}_{tax_counter}"

                    record = {
                        "tax_surcharge_id": tax_id,
                        "policy_id": policy_id,
                        "line_id": None,
                        "tax_state": self._get_text(tax.find("TaxState")),
                        "amount": self._get_float(tax.find("Amount")),
                        "written": self._get_float(tax, tax.get("written")),
                        "change": self._get_float(tax, tax.get("change")),
                        "type": self._get_text(tax.find("Type")),
                        "source_file": source_file,
                        "_extracted_at": extracted_at,
                    }
                    records.append(record)

                # Line-level tax surcharges
                for line in policy.findall("line"):
                    line_id = line.get("id")

                    for tax in line.findall("lineStateTaxSurcharge"):
                        tax_counter += 1
                        tax_id = tax.get("id") or f"tax_{policy_id}_{line_id}_{tax_counter}"

                        record = {
                            "tax_surcharge_id": tax_id,
                            "policy_id": policy_id,
                            "line_id": line_id,
                            "tax_state": self._get_text(tax.find("TaxState")),
                            "amount": self._get_float(tax.find("Amount")),
                            "written": self._get_float(tax, tax.get("written")),
                            "change": self._get_float(tax, tax.get("change")),
                            "type": self._get_text(tax.find("Type")),
                            "source_file": source_file,
                            "_extracted_at": extracted_at,
                        }
                        records.append(record)

        return records
