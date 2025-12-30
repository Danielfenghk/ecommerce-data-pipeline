"""
Data Quality Checks Module
Comprehensive data quality validation framework
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass, field, asdict
from enum import Enum
import re
import json
import hashlib
from collections import defaultdict
import logging

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


class QualityCheckType(Enum):
    """Types of quality checks"""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"
    INTEGRITY = "integrity"


class QualitySeverity(Enum):
    """Severity levels for quality issues"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class QualityResult:
    """Result of a quality check"""
    rule_name: str
    check_type: str
    passed: bool
    actual_value: Any
    expected_threshold: Any
    severity: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)


@dataclass
class QualityReport:
    """Complete quality report for a dataset"""
    dataset_name: str
    total_records: int
    check_results: List[QualityResult]
    overall_score: float
    execution_time_seconds: float
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def passed_checks(self) -> int:
        return sum(1 for r in self.check_results if r.passed)
    
    @property
    def failed_checks(self) -> int:
        return sum(1 for r in self.check_results if not r.passed)
    
    @property
    def critical_failures(self) -> List[QualityResult]:
        return [r for r in self.check_results 
                if not r.passed and r.severity == QualitySeverity.CRITICAL.value]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'dataset_name': self.dataset_name,
            'total_records': self.total_records,
            'check_results': [r.to_dict() for r in self.check_results],
            'overall_score': self.overall_score,
            'passed_checks': self.passed_checks,
            'failed_checks': self.failed_checks,
            'execution_time_seconds': self.execution_time_seconds,
            'timestamp': self.timestamp,
            'metadata': self.metadata
        }
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str, indent=2)
    
    def get_summary(self) -> str:
        """Get a human-readable summary"""
        return f"""
Quality Report: {self.dataset_name}
{'='*50}
Total Records: {self.total_records:,}
Overall Score: {self.overall_score:.2%}
Checks Passed: {self.passed_checks}/{len(self.check_results)}
Critical Failures: {len(self.critical_failures)}
Execution Time: {self.execution_time_seconds:.2f}s
Timestamp: {self.timestamp}
"""


class DataQualityChecker:
    """
    Comprehensive data quality checker supporting multiple validation types
    """
    
    def __init__(self, df: pd.DataFrame, dataset_name: str = "unknown"):
        """
        Initialize the quality checker
        
        Args:
            df: DataFrame to check
            dataset_name: Name of the dataset for reporting
        """
        self.df = df
        self.dataset_name = dataset_name
        self.results: List[QualityResult] = []
        self._start_time = None
        
    def _record_result(self, result: QualityResult):
        """Record a quality check result"""
        self.results.append(result)
        log_level = logging.WARNING if not result.passed else logging.DEBUG
        logger.log(log_level, f"Quality check '{result.rule_name}': {'PASSED' if result.passed else 'FAILED'}")
        return result
    
    # ==================== Completeness Checks ====================
    
    def check_not_null(self, column: str, threshold: float = 1.0,
                       severity: QualitySeverity = QualitySeverity.HIGH) -> QualityResult:
        """
        Check that a column has no null values (or meets threshold)
        
        Args:
            column: Column name to check
            threshold: Minimum ratio of non-null values (0-1)
            severity: Severity level if check fails
        """
        if column not in self.df.columns:
            return self._record_result(QualityResult(
                rule_name=f"not_null_{column}",
                check_type=QualityCheckType.COMPLETENESS.value,
                passed=False,
                actual_value=0,
                expected_threshold=threshold,
                severity=severity.value,
                details={'error': f"Column '{column}' not found"}
            ))
        
        non_null_ratio = self.df[column].notna().mean()
        passed = non_null_ratio >= threshold
        
        return self._record_result(QualityResult(
            rule_name=f"not_null_{column}",
            check_type=QualityCheckType.COMPLETENESS.value,
            passed=passed,
            actual_value=non_null_ratio,
            expected_threshold=threshold,
            severity=severity.value,
            details={
                'null_count': int(self.df[column].isna().sum()),
                'total_count': len(self.df),
                'non_null_percentage': f"{non_null_ratio:.2%}"
            }
        ))
    
    def check_completeness(self, columns: List[str] = None,
                          threshold: float = 0.95,
                          severity: QualitySeverity = QualitySeverity.MEDIUM) -> QualityResult:
        """
        Check overall completeness of specified columns or entire dataset
        """
        if columns is None:
            columns = self.df.columns.tolist()
        
        missing_cols = [c for c in columns if c not in self.df.columns]
        if missing_cols:
            return self._record_result(QualityResult(
                rule_name="completeness_check",
                check_type=QualityCheckType.COMPLETENESS.value,
                passed=False,
                actual_value=0,
                expected_threshold=threshold,
                severity=severity.value,
                details={'error': f"Columns not found: {missing_cols}"}
            ))
        
        completeness_by_column = {}
        for col in columns:
            completeness_by_column[col] = float(self.df[col].notna().mean())
        
        overall_completeness = np.mean(list(completeness_by_column.values()))
        passed = overall_completeness >= threshold
        
        return self._record_result(QualityResult(
            rule_name="completeness_check",
            check_type=QualityCheckType.COMPLETENESS.value,
            passed=passed,
            actual_value=overall_completeness,
            expected_threshold=threshold,
            severity=severity.value,
            details={
                'completeness_by_column': completeness_by_column,
                'columns_below_threshold': [
                    col for col, val in completeness_by_column.items() 
                    if val < threshold
                ]
            }
        ))
    
    # ==================== Uniqueness Checks ====================
    
    def check_unique(self, column: str, threshold: float = 1.0,
                    severity: QualitySeverity = QualitySeverity.HIGH) -> QualityResult:
        """
        Check that a column has unique values
        """
        if column not in self.df.columns:
            return self._record_result(QualityResult(
                rule_name=f"unique_{column}",
                check_type=QualityCheckType.UNIQUENESS.value,
                passed=False,
                actual_value=0,
                expected_threshold=threshold,
                severity=severity.value,
                details={'error': f"Column '{column}' not found"}
            ))
        
        non_null_df = self.df[self.df[column].notna()]
        if len(non_null_df) == 0:
            unique_ratio = 1.0
        else:
            unique_ratio = non_null_df[column].nunique() / len(non_null_df)
        
        passed = unique_ratio >= threshold
        
        # Find duplicates for details
        duplicates = non_null_df[non_null_df[column].duplicated(keep=False)]
        duplicate_values = duplicates[column].value_counts().head(10).to_dict()
        
        return self._record_result(QualityResult(
            rule_name=f"unique_{column}",
            check_type=QualityCheckType.UNIQUENESS.value,
            passed=passed,
            actual_value=unique_ratio,
            expected_threshold=threshold,
            severity=severity.value,
            details={
                'unique_count': int(non_null_df[column].nunique()),
                'total_count': len(non_null_df),
                'duplicate_count': len(duplicates),
                'top_duplicates': duplicate_values
            }
        ))
    
    def check_primary_key(self, columns: Union[str, List[str]],
                         severity: QualitySeverity = QualitySeverity.CRITICAL) -> QualityResult:
        """
        Check that specified column(s) can serve as a primary key
        (unique and not null)
        """
        if isinstance(columns, str):
            columns = [columns]
        
        missing_cols = [c for c in columns if c not in self.df.columns]
        if missing_cols:
            return self._record_result(QualityResult(
                rule_name=f"primary_key_{'_'.join(columns)}",
                check_type=QualityCheckType.UNIQUENESS.value,
                passed=False,
                actual_value=0,
                expected_threshold=1.0,
                severity=severity.value,
                details={'error': f"Columns not found: {missing_cols}"}
            ))
        
        # Check for nulls
        null_mask = self.df[columns].isna().any(axis=1)
        null_count = null_mask.sum()
        
        # Check for duplicates
        non_null_df = self.df[~null_mask]
        if len(columns) == 1:
            duplicate_mask = non_null_df[columns[0]].duplicated(keep=False)
        else:
            duplicate_mask = non_null_df.duplicated(subset=columns, keep=False)
        
        duplicate_count = duplicate_mask.sum()
        
        passed = null_count == 0 and duplicate_count == 0
        
        return self._record_result(QualityResult(
            rule_name=f"primary_key_{'_'.join(columns)}",
            check_type=QualityCheckType.UNIQUENESS.value,
            passed=passed,
            actual_value=1.0 if passed else 0.0,
            expected_threshold=1.0,
            severity=severity.value,
            details={
                'null_count': int(null_count),
                'duplicate_count': int(duplicate_count),
                'total_records': len(self.df),
                'columns_checked': columns
            }
        ))
    
    # ==================== Validity Checks ====================
    
    def check_values_in_set(self, column: str, valid_values: List[Any],
                           threshold: float = 1.0,
                           severity: QualitySeverity = QualitySeverity.MEDIUM) -> QualityResult:
        """
        Check that column values are within a valid set
        """
        if column not in self.df.columns:
            return self._record_result(QualityResult(
                rule_name=f"values_in_set_{column}",
                check_type=QualityCheckType.VALIDITY.value,
                passed=False,
                actual_value=0,
                expected_threshold=threshold,
                severity=severity.value,
                details={'error': f"Column '{column}' not found"}
            ))
        
        non_null_values = self.df[column].dropna()
        if len(non_null_values) == 0:
            valid_ratio = 1.0
        else:
            valid_mask = non_null_values.isin(valid_values)
            valid_ratio = valid_mask.mean()
        
        passed = valid_ratio >= threshold
        
        # Find invalid values
        invalid_values = non_null_values[~non_null_values.isin(valid_values)]
        invalid_value_counts = invalid_values.value_counts().head(10).to_dict()
        
        return self._record_result(QualityResult(
            rule_name=f"values_in_set_{column}",
            check_type=QualityCheckType.VALIDITY.value,
            passed=passed,
            actual_value=valid_ratio,
            expected_threshold=threshold,
            severity=severity.value,
            details={
                'valid_values': valid_values,
                'invalid_count': len(invalid_values),
                'top_invalid_values': invalid_value_counts
            }
        ))
    
    def check_range(self, column: str, min_value: Any = None, max_value: Any = None,
                   threshold: float = 1.0,
                   severity: QualitySeverity = QualitySeverity.MEDIUM) -> QualityResult:
        """
        Check that numeric column values are within a range
        """
        if column not in self.df.columns:
            return self._record_result(QualityResult(
                rule_name=f"range_{column}",
                check_type=QualityCheckType.VALIDITY.value,
                passed=False,
                actual_value=0,
                expected_threshold=threshold,
                severity=severity.value,
                details={'error': f"Column '{column}' not found"}
            ))
        
        non_null_values = self.df[column].dropna()
        if len(non_null_values) == 0:
            in_range_ratio = 1.0
        else:
            in_range_mask = pd.Series([True] * len(non_null_values), index=non_null_values.index)
            
            if min_value is not None:
                in_range_mask &= non_null_values >= min_value
            if max_value is not None:
                in_range_mask &= non_null_values <= max_value
            
            in_range_ratio = in_range_mask.mean()
        
        passed = in_range_ratio >= threshold
        
        # Statistics about out of range values
        out_of_range = non_null_values[~in_range_mask] if len(non_null_values) > 0 else pd.Series()
        
        return self._record_result(QualityResult(
            rule_name=f"range_{column}",
            check_type=QualityCheckType.VALIDITY.value,
            passed=passed,
            actual_value=in_range_ratio,
            expected_threshold=threshold,
            severity=severity.value,
            details={
                'min_allowed': min_value,
                'max_allowed': max_value,
                'actual_min': float(non_null_values.min()) if len(non_null_values) > 0 else None,
                'actual_max': float(non_null_values.max()) if len(non_null_values) > 0 else None,
                'out_of_range_count': len(out_of_range)
            }
        ))
    
    def check_regex(self, column: str, pattern: str, threshold: float = 1.0,
                   severity: QualitySeverity = QualitySeverity.MEDIUM) -> QualityResult:
        """
        Check that string values match a regex pattern
        """
        if column not in self.df.columns:
            return self._record_result(QualityResult(
                rule_name=f"regex_{column}",
                check_type=QualityCheckType.VALIDITY.value,
                passed=False,
                actual_value=0,
                expected_threshold=threshold,
                severity=severity.value,
                details={'error': f"Column '{column}' not found"}
            ))
        
        non_null_values = self.df[column].dropna().astype(str)
        if len(non_null_values) == 0:
            match_ratio = 1.0
        else:
            regex = re.compile(pattern)
            matches = non_null_values.apply(lambda x: bool(regex.match(str(x))))
            match_ratio = matches.mean()
        
        passed = match_ratio >= threshold
        
        # Find non-matching values
        non_matching = non_null_values[~matches] if len(non_null_values) > 0 else pd.Series()
        
        return self._record_result(QualityResult(
            rule_name=f"regex_{column}",
            check_type=QualityCheckType.VALIDITY.value,
            passed=passed,
            actual_value=match_ratio,
            expected_threshold=threshold,
            severity=severity.value,
            details={
                'pattern': pattern,
                'non_matching_count': len(non_matching),
                'sample_non_matching': non_matching.head(5).tolist()
            }
        ))
    
    def check_email_format(self, column: str, threshold: float = 1.0,
                          severity: QualitySeverity = QualitySeverity.MEDIUM) -> QualityResult:
        """
        Check that values are valid email addresses
        """
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        result = self.check_regex(column, email_pattern, threshold, severity)
        result.rule_name = f"email_format_{column}"
        return result
    
    def check_phone_format(self, column: str, threshold: float = 1.0,
                          severity: QualitySeverity = QualitySeverity.MEDIUM) -> QualityResult:
        """
        Check that values are valid phone numbers
        """
        phone_pattern = r'^[\+]?[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}$'
        result = self.check_regex(column, phone_pattern, threshold, severity)
        result.rule_name = f"phone_format_{column}"
        return result
    
    # ==================== Timeliness Checks ====================
    
    def check_freshness(self, column: str, max_age_hours: int = 24,
                       severity: QualitySeverity = QualitySeverity.HIGH) -> QualityResult:
        """Check that data is fresh (within time limit)"""
        if column not in self.df.columns:
            return self._record_result(QualityResult(
                rule_name=f"freshness_{column}",
                check_type=QualityCheckType.TIMELINESS.value,
                passed=False,
                actual_value=0,
                expected_threshold=max_age_hours,
                severity=severity.value,
                details={'error': f"Column '{column}' not found"}
            ))
        
        try:
            max_timestamp = pd.to_datetime(self.df[column]).max()
            if pd.isna(max_timestamp):
                return self._record_result(QualityResult(
                    rule_name=f"freshness_{column}",
                    check_type=QualityCheckType.TIMELINESS.value,
                    passed=False,
                    actual_value=None,
                    expected_threshold=max_age_hours,
                    severity=severity.value,
                    details={'error': 'No valid timestamps found'}
                ))
            
            current_time = datetime.now()
            age_hours = (current_time - max_timestamp).total_seconds() / 3600
            passed = age_hours <= max_age_hours
            
            return self._record_result(QualityResult(
                rule_name=f"freshness_{column}",
                check_type=QualityCheckType.TIMELINESS.value,
                passed=passed,
                actual_value=age_hours,
                expected_threshold=max_age_hours,
                severity=severity.value,
                details={
                    'max_timestamp': str(max_timestamp),
                    'current_time': str(current_time),
                    'age_hours': round(age_hours, 2),
                    'age_days': round(age_hours / 24, 2)
                }
            ))
            
        except Exception as e:
            return self._record_result(QualityResult(
                rule_name=f"freshness_{column}",
                check_type=QualityCheckType.TIMELINESS.value,
                passed=False,
                actual_value=None,
                expected_threshold=max_age_hours,
                severity=severity.value,
                details={'error': str(e)}
            ))
    
    def check_no_future_dates(self, column: str, threshold: float = 1.0,
                             severity: QualitySeverity = QualitySeverity.HIGH) -> QualityResult:
        """
        Check that date column has no future dates
        """
        if column not in self.df.columns:
            return self._record_result(QualityResult(
                rule_name=f"no_future_dates_{column}",
                check_type=QualityCheckType.TIMELINESS.value,
                passed=False,
                actual_value=0,
                expected_threshold=threshold,
                severity=severity.value,
                details={'error': f"Column '{column}' not found"}
            ))
        
        try:
            dates = pd.to_datetime(self.df[column], errors='coerce')
            non_null_dates = dates.dropna()
            
            if len(non_null_dates) == 0:
                valid_ratio = 1.0
            else:
                current_time = datetime.now()
                not_future_mask = non_null_dates <= current_time
                valid_ratio = not_future_mask.mean()
            
            passed = valid_ratio >= threshold
            
            future_dates = non_null_dates[~not_future_mask] if len(non_null_dates) > 0 else pd.Series()
            
            return self._record_result(QualityResult(
                rule_name=f"no_future_dates_{column}",
                check_type=QualityCheckType.TIMELINESS.value,
                passed=passed,
                actual_value=valid_ratio,
                expected_threshold=threshold,
                severity=severity.value,
                details={
                    'future_date_count': len(future_dates),
                    'sample_future_dates': [str(d) for d in future_dates.head(5).tolist()]
                }
            ))
            
        except Exception as e:
            return self._record_result(QualityResult(
                rule_name=f"no_future_dates_{column}",
                check_type=QualityCheckType.TIMELINESS.value,
                passed=False,
                actual_value=None,
                expected_threshold=threshold,
                severity=severity.value,
                details={'error': str(e)}
            ))
    
    # ==================== Consistency Checks ====================
    
    def check_referential_integrity(self, column: str, reference_values: List[Any],
                                   threshold: float = 1.0,
                                   severity: QualitySeverity = QualitySeverity.HIGH) -> QualityResult:
        """
        Check that column values exist in a reference list (foreign key check)
        """
        if column not in self.df.columns:
            return self._record_result(QualityResult(
                rule_name=f"referential_integrity_{column}",
                check_type=QualityCheckType.INTEGRITY.value,
                passed=False,
                actual_value=0,
                expected_threshold=threshold,
                severity=severity.value,
                details={'error': f"Column '{column}' not found"}
            ))
        
        non_null_values = self.df[column].dropna()
        if len(non_null_values) == 0:
            valid_ratio = 1.0
        else:
            valid_mask = non_null_values.isin(reference_values)
            valid_ratio = valid_mask.mean()
        
        passed = valid_ratio >= threshold
        
        # Find orphaned values
        orphaned = non_null_values[~non_null_values.isin(reference_values)]
        
        return self._record_result(QualityResult(
            rule_name=f"referential_integrity_{column}",
            check_type=QualityCheckType.INTEGRITY.value,
            passed=passed,
            actual_value=valid_ratio,
            expected_threshold=threshold,
            severity=severity.value,
            details={
                'orphaned_count': len(orphaned),
                'sample_orphaned': orphaned.unique()[:10].tolist(),
                'reference_count': len(reference_values)
            }
        ))
    
    def check_cross_field_consistency(self, condition: str, 
                                     description: str = "Cross-field check",
                                     threshold: float = 1.0,
                                     severity: QualitySeverity = QualitySeverity.MEDIUM) -> QualityResult:
        """
        Check consistency between multiple fields using a condition expression
        
        Example: condition="order_total == quantity * unit_price"
        """
        try:
            mask = self.df.eval(condition)
            valid_ratio = mask.mean()
            passed = valid_ratio >= threshold
            
            invalid_count = (~mask).sum()
            
            return self._record_result(QualityResult(
                rule_name=f"cross_field_{description.replace(' ', '_').lower()}",
                check_type=QualityCheckType.CONSISTENCY.value,
                passed=passed,
                actual_value=valid_ratio,
                expected_threshold=threshold,
                severity=severity.value,
                details={
                    'condition': condition,
                    'description': description,
                    'invalid_count': int(invalid_count)
                }
            ))
            
        except Exception as e:
            return self._record_result(QualityResult(
                rule_name=f"cross_field_{description.replace(' ', '_').lower()}",
                check_type=QualityCheckType.CONSISTENCY.value,
                passed=False,
                actual_value=None,
                expected_threshold=threshold,
                severity=severity.value,
                details={'error': str(e), 'condition': condition}
            ))
    
    # ==================== Accuracy Checks ====================
    
    def check_statistical_outliers(self, column: str, method: str = 'iqr',
                                  threshold: float = 0.95,
                                  severity: QualitySeverity = QualitySeverity.LOW) -> QualityResult:
        """
        Check for statistical outliers using IQR or Z-score method
        """
        if column not in self.df.columns:
            return self._record_result(QualityResult(
                rule_name=f"outliers_{column}",
                check_type=QualityCheckType.ACCURACY.value,
                passed=False,
                actual_value=0,
                expected_threshold=threshold,
                severity=severity.value,
                details={'error': f"Column '{column}' not found"}
            ))
        
        non_null_values = pd.to_numeric(self.df[column], errors='coerce').dropna()
        if len(non_null_values) == 0:
            return self._record_result(QualityResult(
                rule_name=f"outliers_{column}",
                check_type=QualityCheckType.ACCURACY.value,
                passed=True,
                actual_value=1.0,
                expected_threshold=threshold,
                severity=severity.value,
                details={'message': 'No numeric values to check'}
            ))
        
        if method == 'iqr':
            Q1 = non_null_values.quantile(0.25)
            Q3 = non_null_values.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            non_outlier_mask = (non_null_values >= lower_bound) & (non_null_values <= upper_bound)
        elif method == 'zscore':
            mean = non_null_values.mean()
            std = non_null_values.std()
            z_scores = np.abs((non_null_values - mean) / std)
            non_outlier_mask = z_scores <= 3
        else:
            raise ValueError(f"Unknown method: {method}")
        
        non_outlier_ratio = non_outlier_mask.mean()
        passed = non_outlier_ratio >= threshold
        
        outliers = non_null_values[~non_outlier_mask]
        
        return self._record_result(QualityResult(
            rule_name=f"outliers_{column}",
            check_type=QualityCheckType.ACCURACY.value,
            passed=passed,
            actual_value=non_outlier_ratio,
            expected_threshold=threshold,
            severity=severity.value,
            details={
                'method': method,
                'outlier_count': len(outliers),
                'outlier_percentage': f"{(1 - non_outlier_ratio):.2%}",
                'sample_outliers': outliers.head(10).tolist(),
                'stats': {
                    'mean': float(non_null_values.mean()),
                    'std': float(non_null_values.std()),
                    'min': float(non_null_values.min()),
                    'max': float(non_null_values.max())
                }
            }
        ))
    
    # ==================== Custom Checks ====================
    
    def check_custom(self, rule_name: str, condition_func: Callable[[pd.DataFrame], pd.Series],
                    description: str = "",
                    threshold: float = 1.0,
                    severity: QualitySeverity = QualitySeverity.MEDIUM) -> QualityResult:
        """
        Apply a custom validation function
        
        Args:
            rule_name: Name for the rule
            condition_func: Function that takes DataFrame and returns boolean Series
            description: Description of the check
            threshold: Minimum ratio that must pass
            severity: Severity level
        """
        try:
            mask = condition_func(self.df)
            valid_ratio = mask.mean()
            passed = valid_ratio >= threshold
            
            return self._record_result(QualityResult(
                rule_name=rule_name,
                check_type=QualityCheckType.VALIDITY.value,
                passed=passed,
                actual_value=valid_ratio,
                expected_threshold=threshold,
                severity=severity.value,
                details={
                    'description': description,
                    'invalid_count': int((~mask).sum())
                }
            ))
            
        except Exception as e:
            return self._record_result(QualityResult(
                rule_name=rule_name,
                check_type=QualityCheckType.VALIDITY.value,
                passed=False,
                actual_value=None,
                expected_threshold=threshold,
                severity=severity.value,
                details={'error': str(e)}
            ))
    
    # ==================== Report Generation ====================
    
    def run_all_checks(self) -> 'DataQualityChecker':
        """
        Run a standard suite of checks based on column types
        """
        self._start_time = datetime.now()
        
        for column in self.df.columns:
            # Check for nulls
            self.check_not_null(column, threshold=0.95, severity=QualitySeverity.MEDIUM)
            
            # Type-specific checks
            if pd.api.types.is_numeric_dtype(self.df[column]):
                self.check_statistical_outliers(column, severity=QualitySeverity.LOW)
            
            if pd.api.types.is_datetime64_any_dtype(self.df[column]):
                self.check_no_future_dates(column, severity=QualitySeverity.MEDIUM)
        
        return self
    
    def generate_report(self) -> QualityReport:
        """
        Generate a comprehensive quality report
        """
        end_time = datetime.now()
        
        if self._start_time:
            execution_time = (end_time - self._start_time).total_seconds()
        else:
            execution_time = 0.0
        
        # Calculate overall score
        if len(self.results) > 0:
            # Weight by severity
            severity_weights = {
                QualitySeverity.LOW.value: 0.25,
                QualitySeverity.MEDIUM.value: 0.5,
                QualitySeverity.HIGH.value: 0.75,
                QualitySeverity.CRITICAL.value: 1.0
            }
            
            weighted_scores = []
            total_weight = 0
            
            for result in self.results:
                weight = severity_weights.get(result.severity, 0.5)
                score = 1.0 if result.passed else 0.0
                weighted_scores.append(score * weight)
                total_weight += weight
            
            overall_score = sum(weighted_scores) / total_weight if total_weight > 0 else 0.0
        else:
            overall_score = 1.0
        
        return QualityReport(
            dataset_name=self.dataset_name,
            total_records=len(self.df),
            check_results=self.results.copy(),
            overall_score=overall_score,
            execution_time_seconds=execution_time,
            metadata={
                'columns': list(self.df.columns),
                'column_types': {col: str(dtype) for col, dtype in self.df.dtypes.items()}
            }
        )
    
    def clear_results(self):
        """Clear all recorded results"""
        self.results = []
        self._start_time = None


class DataQualityPipeline:
    """
    Pipeline for running quality checks across multiple datasets
    """
    
    def __init__(self):
        self.reports: Dict[str, QualityReport] = {}
        self.quality_thresholds = {
            'completeness': 0.95,
            'uniqueness': 0.99,
            'validity': 0.98,
            'freshness_hours': 24
        }
    
    def set_thresholds(self, **kwargs):
        """Set quality thresholds"""
        self.quality_thresholds.update(kwargs)
    
    def check_orders(self, df: pd.DataFrame) -> QualityReport:
        """Run quality checks specific to orders data"""
        checker = DataQualityChecker(df, "orders")
        checker._start_time = datetime.now()
        
        # Primary key check
        checker.check_primary_key('order_id', severity=QualitySeverity.CRITICAL)
        
        # Required fields
        for col in ['order_id', 'customer_id', 'order_date', 'total_amount']:
            checker.check_not_null(col, threshold=1.0, severity=QualitySeverity.HIGH)
        
        # Valid values
        checker.check_values_in_set(
            'status',
            ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded'],
            severity=QualitySeverity.MEDIUM
        )
        
        # Range checks
        checker.check_range('total_amount', min_value=0, severity=QualitySeverity.HIGH)
        checker.check_range('quantity', min_value=1, severity=QualitySeverity.HIGH)
        
        # Date checks
        checker.check_no_future_dates('order_date', severity=QualitySeverity.HIGH)
        checker.check_freshness('order_date', max_age_hours=self.quality_thresholds['freshness_hours'])
        
        # Cross-field consistency
        if 'quantity' in df.columns and 'unit_price' in df.columns:
            checker.check_cross_field_consistency(
                'total_amount >= quantity * unit_price * 0.9',
                description='Total amount consistency'
            )
        
        report = checker.generate_report()
        self.reports['orders'] = report
        return report
    
    def check_customers(self, df: pd.DataFrame) -> QualityReport:
        """Run quality checks specific to customers data"""
        checker = DataQualityChecker(df, "customers")
        checker._start_time = datetime.now()
        
        # Primary key
        checker.check_primary_key('customer_id', severity=QualitySeverity.CRITICAL)
        
        # Required fields
        for col in ['customer_id', 'email']:
            checker.check_not_null(col, threshold=1.0, severity=QualitySeverity.HIGH)
        
        # Email format
        checker.check_email_format('email', threshold=0.99, severity=QualitySeverity.MEDIUM)
        
        # Uniqueness
        checker.check_unique('email', threshold=1.0, severity=QualitySeverity.HIGH)
        
        # Phone format (if exists)
        if 'phone' in df.columns:
            checker.check_phone_format('phone', threshold=0.95, severity=QualitySeverity.LOW)
        
        report = checker.generate_report()
        self.reports['customers'] = report
        return report
    
    def check_products(self, df: pd.DataFrame) -> QualityReport:
        """Run quality checks specific to products data"""
        checker = DataQualityChecker(df, "products")
        checker._start_time = datetime.now()
        
        # Primary key
        checker.check_primary_key('product_id', severity=QualitySeverity.CRITICAL)
        
        # Required fields
        for col in ['product_id', 'name', 'price']:
            checker.check_not_null(col, threshold=1.0, severity=QualitySeverity.HIGH)
        
        # Range checks
        checker.check_range('price', min_value=0, severity=QualitySeverity.HIGH)
        checker.check_range('stock_quantity', min_value=0, severity=QualitySeverity.MEDIUM)
        
        # Category validation
        if 'category' in df.columns:
            checker.check_not_null('category', threshold=0.95, severity=QualitySeverity.MEDIUM)
        
        report = checker.generate_report()
        self.reports['products'] = report
        return report
    
    def get_overall_summary(self) -> Dict[str, Any]:
        """Get summary across all checked datasets"""
        if not self.reports:
            return {'message': 'No reports generated yet'}
        
        total_checks = sum(len(r.check_results) for r in self.reports.values())
        total_passed = sum(r.passed_checks for r in self.reports.values())
        total_failed = sum(r.failed_checks for r in self.reports.values())
        
        all_critical_failures = []
        for name, report in self.reports.items():
            for failure in report.critical_failures:
                all_critical_failures.append({
                    'dataset': name,
                    'rule': failure.rule_name,
                    'details': failure.details
                })
        
        avg_score = np.mean([r.overall_score for r in self.reports.values()])
        
        return {
            'datasets_checked': list(self.reports.keys()),
            'total_checks': total_checks,
            'passed': total_passed,
            'failed': total_failed,
            'pass_rate': total_passed / total_checks if total_checks > 0 else 1.0,
            'average_score': avg_score,
            'critical_failures': all_critical_failures,
            'has_critical_failures': len(all_critical_failures) > 0
        }


# Factory functions for common checks
def create_standard_order_checks() -> List[Dict[str, Any]]:
    """Create standard check configuration for orders"""
    return [
        {'check': 'primary_key', 'column': 'order_id'},
        {'check': 'not_null', 'column': 'customer_id'},
        {'check': 'not_null', 'column': 'order_date'},
        {'check': 'range', 'column': 'total_amount', 'min_value': 0},
        {'check': 'values_in_set', 'column': 'status', 
         'valid_values': ['pending', 'processing', 'shipped', 'delivered', 'cancelled']}
    ]


def create_standard_customer_checks() -> List[Dict[str, Any]]:
    """Create standard check configuration for customers"""
    return [
        {'check': 'primary_key', 'column': 'customer_id'},
        {'check': 'unique', 'column': 'email'},
        {'check': 'email_format', 'column': 'email'},
        {'check': 'not_null', 'column': 'email'}
    ]


if __name__ == "__main__":
    # Example usage
    import pandas as pd
    
    # Create sample data
    orders_data = {
        'order_id': [1, 2, 3, 4, 5],
        'customer_id': [101, 102, 103, 101, None],
        'order_date': pd.to_datetime(['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19']),
        'total_amount': [150.00, 200.50, -10.00, 500.00, 75.25],
        'quantity': [2, 3, 1, 5, 1],
        'status': ['delivered', 'shipped', 'invalid_status', 'pending', 'processing']
    }
    orders_df = pd.DataFrame(orders_data)
    
    # Run quality checks
    pipeline = DataQualityPipeline()
    report = pipeline.check_orders(orders_df)
    
    print(report.get_summary())
    print("\nDetailed Results:")
    for result in report.check_results:
        status = "✓" if result.passed else "✗"
        print(f"  {status} {result.rule_name}: {result.actual_value}")