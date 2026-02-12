"""
Metrics collection and tracking utilities.
"""
from typing import Dict, Any
from datetime import datetime
import time


class MetricsCollector:
    """Collects and tracks execution metrics."""

    def __init__(self):
        """Initialize metrics collector."""
        self.metrics: Dict[str, Any] = {
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0,
            'validations_executed': 0,
            'validations_passed': 0,
            'validations_failed': 0,
            'validations_with_errors': 0,
            'connectors_used': set(),
            'validators_used': set(),
            'reporters_used': set(),
        }
        self._timers: Dict[str, float] = {}

    def start(self) -> None:
        """Start tracking metrics."""
        self.metrics['start_time'] = datetime.now()

    def end(self) -> None:
        """End tracking metrics."""
        self.metrics['end_time'] = datetime.now()
        if self.metrics['start_time']:
            duration = self.metrics['end_time'] - self.metrics['start_time']
            self.metrics['duration_seconds'] = duration.total_seconds()

    def record_validation(self, status: str) -> None:
        """Record validation execution."""
        self.metrics['validations_executed'] += 1
        if status.lower() == 'passed':
            self.metrics['validations_passed'] += 1
        elif status.lower() in ['failed', 'error']:
            self.metrics['validations_failed'] += 1

    def record_connector(self, connector_name: str) -> None:
        """Record connector usage."""
        self.metrics['connectors_used'].add(connector_name)

    def record_validator(self, validator_type: str) -> None:
        """Record validator usage."""
        self.metrics['validators_used'].add(validator_type)

    def record_reporter(self, reporter_type: str) -> None:
        """Record reporter usage."""
        self.metrics['reporters_used'].add(reporter_type)

    def start_timer(self, name: str) -> None:
        """Start a named timer."""
        self._timers[name] = time.time()

    def stop_timer(self, name: str) -> float:
        """
        Stop a named timer and return elapsed time.

        Args:
            name: Timer name

        Returns:
            Elapsed time in seconds
        """
        if name in self._timers:
            elapsed = time.time() - self._timers[name]
            del self._timers[name]
            return elapsed
        return 0.0

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get collected metrics.

        Returns:
            Dictionary of metrics
        """
        # Convert sets to lists for JSON serialization
        metrics_copy = self.metrics.copy()
        metrics_copy['connectors_used'] = list(metrics_copy['connectors_used'])
        metrics_copy['validators_used'] = list(metrics_copy['validators_used'])
        metrics_copy['reporters_used'] = list(metrics_copy['reporters_used'])

        # Format timestamps
        if metrics_copy['start_time']:
            metrics_copy['start_time'] = metrics_copy['start_time'].isoformat()
        if metrics_copy['end_time']:
            metrics_copy['end_time'] = metrics_copy['end_time'].isoformat()

        return metrics_copy

    def get_summary(self) -> str:
        """
        Get human-readable metrics summary.

        Returns:
            Formatted summary string
        """
        return (
            f"Metrics Summary:\n"
            f"  Duration: {self.metrics['duration_seconds']:.2f}s\n"
            f"  Validations: {self.metrics['validations_executed']} executed, "
            f"{self.metrics['validations_passed']} passed, "
            f"{self.metrics['validations_failed']} failed\n"
            f"  Connectors: {len(self.metrics['connectors_used'])}\n"
            f"  Validators: {len(self.metrics['validators_used'])}\n"
            f"  Reporters: {len(self.metrics['reporters_used'])}"
        )
