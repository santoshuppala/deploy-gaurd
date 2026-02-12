"""
Email reporter implementation.
Sends validation results via email.
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import List

from .base_reporter import BaseReporter
from ..models.validation_result import ValidationSummary
from ..models.enums import ReporterType


class EmailReporter(BaseReporter):
    """Sends validation reports via email."""

    def get_reporter_type(self) -> ReporterType:
        """Return reporter type."""
        return ReporterType.EMAIL

    def generate_report(self, summary: ValidationSummary) -> str:
        """
        Send email report.

        Args:
            summary: ValidationSummary with all results

        Returns:
            Status message
        """
        # Get email configuration
        smtp_host = self.config.get('smtp_host', 'localhost')
        smtp_port = self.config.get('smtp_port', 587)
        sender = self.config.get('sender')
        recipients = self.config.get('recipients', [])
        subject = self.config.get('subject', f'Validation Report - {datetime.now().strftime("%Y-%m-%d")}')
        use_tls = self.config.get('use_tls', True)
        username = self.config.get('username')
        password = self.config.get('password')

        if not sender or not recipients:
            raise ValueError("Email sender and recipients are required")

        # Generate email content
        html_content = self._generate_email_html(summary)
        text_content = self._generate_email_text(summary)

        # Create message
        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = sender
        message['To'] = ', '.join(recipients)

        # Attach both plain text and HTML versions
        text_part = MIMEText(text_content, 'plain')
        html_part = MIMEText(html_content, 'html')

        message.attach(text_part)
        message.attach(html_part)

        # Send email
        try:
            self.logger.info(f"Sending email to {', '.join(recipients)}")

            with smtplib.SMTP(smtp_host, smtp_port) as server:
                if use_tls:
                    server.starttls()

                if username and password:
                    server.login(username, password)

                server.send_message(message)

            self.logger.info("Email sent successfully")
            return f"Email sent to {len(recipients)} recipient(s)"

        except Exception as e:
            error_msg = f"Failed to send email: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def _generate_email_text(self, summary: ValidationSummary) -> str:
        """Generate plain text email content."""
        text = f"""
VALIDATION REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

SUMMARY
-------
Total Validations: {summary.total_validations}
Passed: {summary.passed}
Warnings: {summary.warnings}
Failed: {summary.failed}
Errors: {summary.errors}
Success Rate: {summary.success_rate:.1f}%
Duration: {summary.total_execution_time_seconds:.2f} seconds

RESULTS
-------
"""

        for idx, result in enumerate(summary.results, 1):
            text += f"\n{idx}. {result.name}\n"
            text += f"   Status: {result.status.value}\n"
            text += f"   Type: {result.validation_type.value}\n"

            if result.source_count is not None:
                text += f"   Source Count: {result.source_count:,}\n"
            if result.target_count is not None:
                text += f"   Target Count: {result.target_count:,}\n"

            if result.error_message:
                text += f"   Error: {result.error_message}\n"

        return text

    def _generate_email_html(self, summary: ValidationSummary) -> str:
        """Generate HTML email content."""
        status_color = '#28a745' if not summary.has_failures() else '#dc3545'
        status_text = 'SUCCESS' if not summary.has_failures() else 'FAILURE'

        if summary.warnings > 0 and not summary.has_failures():
            status_color = '#ffc107'
            status_text = 'SUCCESS WITH WARNINGS'

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 8px;
            text-align: center;
            margin-bottom: 30px;
        }}
        .status-banner {{
            background: {status_color};
            color: white;
            padding: 15px;
            border-radius: 5px;
            text-align: center;
            font-weight: bold;
            font-size: 1.2em;
            margin-bottom: 20px;
        }}
        .summary {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 15px;
        }}
        .summary-item {{
            background: white;
            padding: 15px;
            border-radius: 5px;
            border-left: 4px solid #667eea;
        }}
        .summary-label {{
            color: #666;
            font-size: 0.9em;
        }}
        .summary-value {{
            font-size: 1.5em;
            font-weight: bold;
            color: #333;
        }}
        .result-card {{
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 15px;
        }}
        .result-header {{
            font-weight: bold;
            margin-bottom: 10px;
            font-size: 1.1em;
        }}
        .passed {{ border-left: 4px solid #28a745; }}
        .failed {{ border-left: 4px solid #dc3545; }}
        .warning {{ border-left: 4px solid #ffc107; }}
        .badge {{
            display: inline-block;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: bold;
        }}
        .badge-passed {{ background: #28a745; color: white; }}
        .badge-failed {{ background: #dc3545; color: white; }}
        .badge-warning {{ background: #ffc107; color: #333; }}
        .footer {{
            text-align: center;
            color: #666;
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Validation Report</h1>
        <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>

    <div class="status-banner">
        {status_text}
    </div>

    <div class="summary">
        <h2>Summary</h2>
        <div class="summary-grid">
            <div class="summary-item">
                <div class="summary-label">Total Validations</div>
                <div class="summary-value">{summary.total_validations}</div>
            </div>
            <div class="summary-item">
                <div class="summary-label">Passed</div>
                <div class="summary-value" style="color: #28a745;">{summary.passed}</div>
            </div>
            <div class="summary-item">
                <div class="summary-label">Failed</div>
                <div class="summary-value" style="color: #dc3545;">{summary.failed}</div>
            </div>
            <div class="summary-item">
                <div class="summary-label">Success Rate</div>
                <div class="summary-value">{summary.success_rate:.1f}%</div>
            </div>
        </div>
    </div>

    <h2>Validation Results</h2>
"""

        for result in summary.results:
            status_class = result.status.value.lower()
            badge_class = f'badge-{status_class}'

            html += f"""
    <div class="result-card {status_class}">
        <div class="result-header">
            {result.name}
            <span class="badge {badge_class}">{result.status.value}</span>
        </div>
        <p><strong>Type:</strong> {result.validation_type.value}</p>
        <p><strong>Duration:</strong> {result.execution_time_seconds:.2f}s</p>
"""

            if result.source_count is not None:
                html += f"<p><strong>Source Count:</strong> {result.source_count:,}</p>"
            if result.target_count is not None:
                html += f"<p><strong>Target Count:</strong> {result.target_count:,}</p>"

            if result.error_message:
                html += f'<p style="color: #dc3545;"><strong>Error:</strong> {result.error_message}</p>'

            html += "</div>"

        html += """
    <div class="footer">
        <p>Generated by Validation Framework</p>
    </div>
</body>
</html>
"""

        return html
