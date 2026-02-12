"""
Console reporter implementation.
Outputs validation results to console with formatting.
"""
from typing import Dict, Any

from .base_reporter import BaseReporter
from ..models.validation_result import ValidationSummary, ValidationResult
from ..models.enums import ReporterType, ValidationStatus


class ConsoleReporter(BaseReporter):
    """Generates console output reports."""

    # Color codes for terminal output
    COLORS = {
        'RESET': '\033[0m',
        'BOLD': '\033[1m',
        'GREEN': '\033[92m',
        'YELLOW': '\033[93m',
        'RED': '\033[91m',
        'BLUE': '\033[94m',
        'CYAN': '\033[96m',
    }

    def get_reporter_type(self) -> ReporterType:
        """Return reporter type."""
        return ReporterType.CONSOLE

    def generate_report(self, summary: ValidationSummary) -> str:
        """
        Generate console report from validation summary.

        Args:
            summary: ValidationSummary with all results

        Returns:
            Status message
        """
        verbose = self.config.get('verbose', True)
        use_colors = self.config.get('color_output', True)

        # Print header
        self._print_header(summary, use_colors)

        # Print summary statistics
        self._print_summary_stats(summary, use_colors)

        # Print individual results
        if verbose:
            self._print_individual_results(summary, use_colors)

        # Print footer
        self._print_footer(summary, use_colors)

        return "Console report generated"

    def _print_header(self, summary: ValidationSummary, use_colors: bool) -> None:
        """Print report header."""
        print()
        print(self._colorize("=" * 80, 'BOLD', use_colors))
        print(self._colorize("VALIDATION REPORT", 'BOLD', use_colors).center(80))
        print(self._colorize("=" * 80, 'BOLD', use_colors))
        print()
        print(f"Start Time:  {summary.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End Time:    {summary.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration:    {summary.total_execution_time_seconds:.2f} seconds")
        print()

    def _print_summary_stats(self, summary: ValidationSummary, use_colors: bool) -> None:
        """Print summary statistics."""
        print(self._colorize("SUMMARY", 'BOLD', use_colors))
        print(self._colorize("-" * 80, 'BOLD', use_colors))
        print()

        # Status counts
        print(f"Total Validations: {summary.total_validations}")
        print(self._colorize(f"✓ Passed:          {summary.passed}", 'GREEN', use_colors))

        if summary.warnings > 0:
            print(self._colorize(f"⚠ Warnings:        {summary.warnings}", 'YELLOW', use_colors))

        if summary.failed > 0:
            print(self._colorize(f"✗ Failed:          {summary.failed}", 'RED', use_colors))

        if summary.errors > 0:
            print(self._colorize(f"✗ Errors:          {summary.errors}", 'RED', use_colors))

        if summary.skipped > 0:
            print(f"○ Skipped:         {summary.skipped}")

        print()
        print(f"Success Rate:      {summary.success_rate:.1f}%")
        print()

    def _print_individual_results(self, summary: ValidationSummary, use_colors: bool) -> None:
        """Print individual validation results."""
        print(self._colorize("VALIDATION RESULTS", 'BOLD', use_colors))
        print(self._colorize("-" * 80, 'BOLD', use_colors))
        print()

        for idx, result in enumerate(summary.results, 1):
            self._print_result(result, idx, use_colors)
            print()

    def _print_result(self, result: ValidationResult, index: int, use_colors: bool) -> None:
        """Print a single validation result."""
        # Status icon and color
        status_info = self._get_status_info(result.status, use_colors)

        # Header
        print(f"{index}. {self._colorize(result.name, 'BOLD', use_colors)}")
        print(f"   Status: {status_info['icon']} {status_info['text']}")
        print(f"   Type:   {result.validation_type.value}")
        print(f"   Time:   {result.execution_time_seconds:.2f}s")

        # Type-specific details
        if result.validation_type.value == 'row_count':
            print(f"   Source: {result.source_count:,} rows")
            print(f"   Target: {result.target_count:,} rows")
            if result.difference is not None:
                print(f"   Diff:   {result.difference:,} ({result.difference_percent:.2f}%)")

        elif result.validation_type.value == 'data_quality':
            if result.null_count is not None:
                print(f"   Nulls:      {result.null_count:,}")
            if result.duplicate_count is not None:
                print(f"   Duplicates: {result.duplicate_count:,}")
            if result.invalid_count is not None:
                print(f"   Invalid:    {result.invalid_count:,}")

        elif result.validation_type.value == 'schema':
            if result.schema_differences:
                print(f"   Schema differences: {len(result.schema_differences)}")
                for diff in result.schema_differences[:3]:  # Show first 3
                    print(f"     - {diff}")
                if len(result.schema_differences) > 3:
                    print(f"     ... and {len(result.schema_differences) - 3} more")

        elif result.validation_type.value == 'business_rule':
            if result.rule_results:
                for key, value in result.rule_results.items():
                    print(f"   {key}: {value}")

        # Error message if present
        if result.error_message:
            print(self._colorize(f"   Error: {result.error_message}", 'RED', use_colors))

    def _print_footer(self, summary: ValidationSummary, use_colors: bool) -> None:
        """Print report footer."""
        print(self._colorize("=" * 80, 'BOLD', use_colors))

        # Overall status
        if summary.has_failures():
            overall = self._colorize("VALIDATION FAILED", 'RED', use_colors)
        elif summary.warnings > 0:
            overall = self._colorize("VALIDATION PASSED WITH WARNINGS", 'YELLOW', use_colors)
        else:
            overall = self._colorize("VALIDATION PASSED", 'GREEN', use_colors)

        print(self._colorize(overall, 'BOLD', use_colors).center(88))
        print(self._colorize("=" * 80, 'BOLD', use_colors))
        print()

    def _get_status_info(self, status: ValidationStatus, use_colors: bool) -> Dict[str, str]:
        """Get status icon and colored text."""
        status_map = {
            ValidationStatus.PASSED: {
                'icon': '✓',
                'text': self._colorize('PASSED', 'GREEN', use_colors)
            },
            ValidationStatus.FAILED: {
                'icon': '✗',
                'text': self._colorize('FAILED', 'RED', use_colors)
            },
            ValidationStatus.WARNING: {
                'icon': '⚠',
                'text': self._colorize('WARNING', 'YELLOW', use_colors)
            },
            ValidationStatus.ERROR: {
                'icon': '✗',
                'text': self._colorize('ERROR', 'RED', use_colors)
            },
            ValidationStatus.SKIPPED: {
                'icon': '○',
                'text': 'SKIPPED'
            }
        }
        return status_map.get(status, {'icon': '?', 'text': str(status)})

    def _colorize(self, text: str, color: str, use_colors: bool) -> str:
        """Apply color to text if colors are enabled."""
        if not use_colors:
            return text
        return f"{self.COLORS.get(color, '')}{text}{self.COLORS['RESET']}"
