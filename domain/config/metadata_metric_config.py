from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetadataMetricSpec:
    indicator: str
    description: str
    dimension: str
    numerator_measure: str
    denominator_measure: str

    # Indicator	Description	             Most appropriate dimension	                  Justification
    # MQID001	Table names in singular	            Consistency	        Checks adherence to a defined naming convention for schema objects.
    # MQID002	Table with recommended name length	Consistency	        Measures compliance with modeling and naming standards.
    # MQID003	Columns with correct prefixes	    Consistency	        Evaluates whether column names follow the expected semantic or technical naming pattern.
    # MQID004	Columns with recommended name size	Consistency	        Also addresses compliance with naming conventions.
    # MQID005	Columns with comments	            Completeness	    Measures whether metadata documentation has been filled in.
    # MQID006	Table with standard PK prefixes	    Consistency	        Verifies standard naming for primary keys.
    # MQID007	Table with standard FK prefixes	    Consistency	        Verifies standard naming for foreign keys.
    # MQID008	Table with standard UK prefixes	    Consistency	        Verifies standard naming for unique keys.
    # MQID009	Columns with valid num_distinct	    Uniqueness          If the validation compares expected cardinality to detect duplication or suspiciously low variability, 
    #                                             (or Consistency,      the best fit is Uniqueness. If it only checks whether the statistic is coherent or available, it could be classified as Consistency.
    #                                           depending on the rule)	                        
    # MQID010	Columns with num_nulls	            Completeness	    Nulls indicate missing expected values.
    # MQID011   Tables with at least one integrity  Consistency         If all tables have at least one PK or UK, then the schema would have 100% for this metric.   
    # MQID012	Identifier-like columns protected   Consistency         The table has columns similar to identifiers protected by PK or UK.
    #           by PK or UK       
    # MQID013   Compliance between type and naming  Consistency         The table has columns with names that suggest a certain data type (e.g., "date", "id", "amount") 
    #           convention                                              and the actual data type of the column is consistent with those expectations.
    # MQID014   Tables with comments                Completeness        Measures whether table-level documentation has been filled in.
    
METADATA_INDICATOR_SPECS: tuple[MetadataMetricSpec, ...] = (
    MetadataMetricSpec("MQID001", "Table names in singular", "Consistency", "MQME012", "MQME001"),
    MetadataMetricSpec("MQID002", "Table with recommended name length", "Consistency", "MQME013", "MQME001"),
    MetadataMetricSpec("MQID003", "Columns with correct prefixes", "Consistency", "MQME014", "MQME002"),
    MetadataMetricSpec("MQID004", "Columns with recommended name size", "Consistency", "MQME015", "MQME002"),
    MetadataMetricSpec("MQID005", "Columns with comments", "Completeness", "MQME008", "MQME002"),
    MetadataMetricSpec("MQID006", "Table with standard PK prefixes", "Consistency", "MQME009", "MQME003"),
    MetadataMetricSpec("MQID007", "Table with standard FK prefixes", "Consistency", "MQME010", "MQME004"),
    MetadataMetricSpec("MQID008", "Table with standard UK prefixes", "Consistency", "MQME011", "MQME005"),
    MetadataMetricSpec("MQID009", "Columns with valid num_distinct", "Uniqueness", "MQME021", "MQME002"),
    MetadataMetricSpec("MQID010", "Columns with num_nulls", "Completeness", "MQME019", "MQME018"),
    MetadataMetricSpec("MQID011", "Tables with at least one integrity constraint", "Consistency", "MQME022", "MQME001"),
    MetadataMetricSpec("MQID012", "Identifier-like columns protected by PK or UK", "Consistency", "MQME024", "MQME023"),
    MetadataMetricSpec("MQID013", "Compliance between type and naming convention", "Consistency", "MQME026", "MQME025"),
    MetadataMetricSpec("MQID014", "Tables with comments", "Completeness", "MQME027", "MQME001"),
)
