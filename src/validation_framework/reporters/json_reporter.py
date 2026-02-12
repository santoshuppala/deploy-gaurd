"""
JSON reporter implementation.
Generates JSON format validation reports.
"""
import json
from datetime import datetime
from pathlib import Path

from .base_reporter import BaseReporter
from ..models.validation_result import ValidationSummary
from ..models.enums import ReporterType


class JSONReporter(BaseReporter):
    """Generates JSON format reports."""

    def get_reporter_type(self) -> ReporterType:
        """Return reporter type."""
        return ReporterType.JSON

    def generate_report(self, summary: ValidationSummary) -> str:
        """
        Generate JSON report from validation summary.

        Args:
            summary: ValidationSummary with all results

        Returns:
            Path to generated JSON file
        """
        # Prepare output path
        if not self.output_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.output_path = f"output/json/validation_results_{timestamp}.json"

        self._ensure_output_directory()

        # Get configuration options
        indent = self.config.get('indent', 2)
        include_metadata = self.config.get('include_metadata', True)

        # Convert summary to dictionary
        report_data = summary.to_dict()

        # Optionally remove metadata
        if not include_metadata:
            for result in report_data.get('results', []):
                result.pop('metadata', None)

        # Write JSON file
        output_file = Path(self.output_path)
        with open(output_file, 'w') as f:
            json.dump(report_data, f, indent=indent, default=str)

        self.logger.info(f"JSON report generated: {output_file}")
        return str(output_file)
