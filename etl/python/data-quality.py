"""
Data Quality Rules for EMS ETL Pipeline
Author: J. Ryan Butcher

Defines validation rules and quality checks for EMS data.
"""

import re
from datetime import datetime
from typing import List, Tuple, Optional, Callable, Dict, Any
from dataclasses import dataclass


@dataclass
class ValidationResult:
    """Result of a validation check."""
    is_valid: bool
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    cleaned_value: Any = None


@dataclass
class ValidationRule:
    """Single validation rule definition."""
    column: str
    rule_name: str
    validator: Callable[[Any], ValidationResult]
    is_required: bool = False
    default_value: Any = None


class DataQualityValidator:
    """
    Data quality validation engine for EMS data.

    Validates individual fields and entire records against
    defined rules, returning cleaned values or error details.
    """

    def __init__(self):
        """Initialize validator with EMS-specific rules."""
        self.rules: Dict[str, List[ValidationRule]] = {}
        self._register_default_rules()

    def _register_default_rules(self):
        """Register default EMS validation rules."""

        # INCIDENT_DT - Required, must be valid date
        self.add_rule(ValidationRule(
            column="INCIDENT_DT",
            rule_name="valid_date",
            validator=self._validate_date,
            is_required=True
        ))

        # INCIDENT_COUNTY - Optional, clean whitespace
        self.add_rule(ValidationRule(
            column="INCIDENT_COUNTY",
            rule_name="clean_text",
            validator=self._validate_text,
            is_required=False
        ))

        # CHIEF_COMPLAINT_DISPATCH - Optional, clean text
        self.add_rule(ValidationRule(
            column="CHIEF_COMPLAINT_DISPATCH",
            rule_name="clean_text",
            validator=self._validate_text,
            is_required=False
        ))

        # Response time fields - must be non-negative
        for col in ["PROVIDER_TO_SCENE_MINS", "PROVIDER_TO_DESTINATION_MINS"]:
            self.add_rule(ValidationRule(
                column=col,
                rule_name="non_negative_number",
                validator=self._validate_non_negative,
                is_required=False
            ))

        # INJURY_FLG - must be Yes/No, map to 1/0
        self.add_rule(ValidationRule(
            column="INJURY_FLG",
            rule_name="yes_no_flag",
            validator=self._validate_yes_no,
            is_required=False,
            default_value=0
        ))

        # NALOXONE_GIVEN_FLG - must be 0/1
        self.add_rule(ValidationRule(
            column="NALOXONE_GIVEN_FLG",
            rule_name="binary_flag",
            validator=self._validate_binary,
            is_required=False,
            default_value=0
        ))

        # MEDICATION_GIVEN_OTHER_FLG - must be 0/1
        self.add_rule(ValidationRule(
            column="MEDICATION_GIVEN_OTHER_FLG",
            rule_name="binary_flag",
            validator=self._validate_binary,
            is_required=False,
            default_value=0
        ))

        # Datetime fields - validate format
        datetime_cols = [
            "UNIT_NOTIFIED_BY_DISPATCH_DT",
            "UNIT_ARRIVED_ON_SCENE_DT",
            "UNIT_ARRIVED_TO_PATIENT_DT",
            "UNIT_LEFT_SCENE_DT",
            "PATIENT_ARRIVED_DESTINATION_DT"
        ]
        for col in datetime_cols:
            self.add_rule(ValidationRule(
                column=col,
                rule_name="valid_datetime",
                validator=self._validate_datetime,
                is_required=False
            ))

    def add_rule(self, rule: ValidationRule):
        """Add a validation rule for a column."""
        if rule.column not in self.rules:
            self.rules[rule.column] = []
        self.rules[rule.column].append(rule)

    def validate_record(
        self,
        record: Dict[str, Any],
        row_num: int
    ) -> Tuple[Dict[str, Any], List[Tuple[str, str, str]]]:
        """
        Validate an entire record.

        Args:
            record: Dictionary of column -> value
            row_num: Source row number for error tracking

        Returns:
            Tuple of (cleaned_record, list of (column, error_type, error_msg))
        """
        cleaned = {}
        errors = []

        for column, value in record.items():
            if column in self.rules:
                for rule in self.rules[column]:
                    result = rule.validator(value)

                    if not result.is_valid:
                        if rule.is_required:
                            errors.append((column, result.error_type, result.error_message))
                        # Use default value if available
                        cleaned[column] = rule.default_value
                    else:
                        cleaned[column] = result.cleaned_value
            else:
                # No rules - pass through with basic cleaning
                cleaned[column] = self._clean_value(value)

        return cleaned, errors

    def _clean_value(self, value: Any) -> Any:
        """Basic value cleaning - trim strings, handle None."""
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            return value if value else None
        return value

    # === Validator Functions ===

    def _validate_date(self, value: Any) -> ValidationResult:
        """Validate date field (YYYY-MM-DD format)."""
        if value is None or (isinstance(value, str) and not value.strip()):
            return ValidationResult(
                is_valid=False,
                error_type="NULL_VALUE",
                error_message="Required date field is null or empty"
            )

        try:
            if isinstance(value, str):
                # Try common formats
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"]:
                    try:
                        parsed = datetime.strptime(value.strip(), fmt)
                        return ValidationResult(
                            is_valid=True,
                            cleaned_value=parsed.strftime("%Y-%m-%d")
                        )
                    except ValueError:
                        continue

                return ValidationResult(
                    is_valid=False,
                    error_type="INVALID_DATE",
                    error_message=f"Cannot parse date: {value}"
                )

            return ValidationResult(is_valid=True, cleaned_value=str(value))

        except Exception as e:
            return ValidationResult(
                is_valid=False,
                error_type="PARSE_ERROR",
                error_message=str(e)
            )

    def _validate_datetime(self, value: Any) -> ValidationResult:
        """Validate datetime field."""
        if value is None or (isinstance(value, str) and not value.strip()):
            return ValidationResult(is_valid=True, cleaned_value=None)

        try:
            if isinstance(value, str):
                # Handle date-only values
                value = value.strip()
                if len(value) == 10:  # YYYY-MM-DD
                    return ValidationResult(is_valid=True, cleaned_value=value)

                # Try datetime formats
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %H:%M"]:
                    try:
                        parsed = datetime.strptime(value, fmt)
                        return ValidationResult(
                            is_valid=True,
                            cleaned_value=parsed.isoformat()
                        )
                    except ValueError:
                        continue

            return ValidationResult(is_valid=True, cleaned_value=str(value))

        except Exception:
            return ValidationResult(is_valid=True, cleaned_value=None)

    def _validate_text(self, value: Any) -> ValidationResult:
        """Validate and clean text field."""
        if value is None:
            return ValidationResult(is_valid=True, cleaned_value=None)

        if isinstance(value, str):
            cleaned = value.strip()
            # Remove excessive whitespace
            cleaned = re.sub(r'\s+', ' ', cleaned)
            return ValidationResult(
                is_valid=True,
                cleaned_value=cleaned if cleaned else None
            )

        return ValidationResult(is_valid=True, cleaned_value=str(value))

    def _validate_non_negative(self, value: Any) -> ValidationResult:
        """Validate numeric field is non-negative."""
        if value is None or (isinstance(value, str) and not value.strip()):
            return ValidationResult(is_valid=True, cleaned_value=None)

        try:
            num = float(value)
            if num < 0:
                return ValidationResult(
                    is_valid=False,
                    error_type="NEGATIVE_VALUE",
                    error_message=f"Value {value} is negative",
                    cleaned_value=None  # Set to NULL instead of rejecting
                )
            return ValidationResult(is_valid=True, cleaned_value=num)

        except (ValueError, TypeError):
            return ValidationResult(
                is_valid=False,
                error_type="INVALID_NUMBER",
                error_message=f"Cannot parse as number: {value}",
                cleaned_value=None
            )

    def _validate_yes_no(self, value: Any) -> ValidationResult:
        """Validate Yes/No field, convert to 1/0."""
        if value is None or (isinstance(value, str) and not value.strip()):
            return ValidationResult(is_valid=True, cleaned_value=0)

        if isinstance(value, str):
            upper = value.strip().upper()
            if upper in ("YES", "Y", "TRUE", "1"):
                return ValidationResult(is_valid=True, cleaned_value=1)
            elif upper in ("NO", "N", "FALSE", "0", ""):
                return ValidationResult(is_valid=True, cleaned_value=0)

        if isinstance(value, (int, float)):
            return ValidationResult(is_valid=True, cleaned_value=1 if value else 0)

        return ValidationResult(
            is_valid=False,
            error_type="INVALID_FLAG",
            error_message=f"Cannot parse Yes/No: {value}",
            cleaned_value=0
        )

    def _validate_binary(self, value: Any) -> ValidationResult:
        """Validate binary (0/1) field."""
        if value is None or (isinstance(value, str) and not value.strip()):
            return ValidationResult(is_valid=True, cleaned_value=0)

        try:
            num = int(float(value))
            if num in (0, 1):
                return ValidationResult(is_valid=True, cleaned_value=num)
            return ValidationResult(
                is_valid=False,
                error_type="INVALID_BINARY",
                error_message=f"Value {value} not 0 or 1",
                cleaned_value=1 if num else 0
            )
        except (ValueError, TypeError):
            return ValidationResult(
                is_valid=False,
                error_type="INVALID_BINARY",
                error_message=f"Cannot parse as binary: {value}",
                cleaned_value=0
            )


# Global validator instance
_validator: Optional[DataQualityValidator] = None


def get_validator() -> DataQualityValidator:
    """Get or create global validator instance."""
    global _validator
    if _validator is None:
        _validator = DataQualityValidator()
    return _validator


if __name__ == "__main__":
    # Test validation
    validator = get_validator()

    test_record = {
        "INCIDENT_DT": "2024-01-15",
        "INCIDENT_COUNTY": "  LAKE  ",
        "INJURY_FLG": "Yes",
        "NALOXONE_GIVEN_FLG": "1",
        "PROVIDER_TO_SCENE_MINS": "5.5"
    }

    cleaned, errors = validator.validate_record(test_record, 1)
    print("Cleaned:", cleaned)
    print("Errors:", errors)
