#!/usr/bin/env python3
"""
CLI entry point for the validation framework.
Provides command-line interface for running validations.
"""
import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import click
from src.validation_framework.core.validation_engine import ValidationEngine
from src.validation_framework.core.config_loader import ConfigLoader
from src.validation_framework.core.exceptions import ValidationFrameworkError


@click.group()
@click.version_option(version='1.0.0', prog_name='Validation Framework')
def cli():
    """
    Pre/Post Deployment Validation Framework for Big Data.

    A production-ready framework for validating data pipelines across
    Spark, Hive, and cloud storage platforms.
    """
    pass


@cli.command()
@click.argument('config_file', type=click.Path(exists=True))
@click.option('--log-level', '-l', default='INFO',
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                               case_sensitive=False),
              help='Set logging level')
@click.option('--output-dir', '-o', type=click.Path(),
              help='Override output directory for reports')
@click.option('--fail-fast', is_flag=True,
              help='Stop on first validation failure')
@click.option('--parallel', is_flag=True,
              help='Execute validations in parallel')
@click.option('--max-workers', '-w', type=int, default=4,
              help='Maximum parallel workers (default: 4)')
def run(config_file, log_level, output_dir, fail_fast, parallel, max_workers):
    """
    Run validations from a configuration file.

    CONFIG_FILE: Path to YAML configuration file

    Example:
        python run_validation.py run config/validation_config.yaml
    """
    try:
        click.echo(f"Starting validation with config: {config_file}")
        click.echo("=" * 80)

        # Override settings if provided via CLI
        if log_level:
            os.environ['LOG_LEVEL'] = log_level
        if fail_fast:
            os.environ['CONTINUE_ON_ERROR'] = 'false'
        if parallel:
            os.environ['PARALLEL_EXECUTION'] = 'true'
        if max_workers:
            os.environ['MAX_WORKERS'] = str(max_workers)
        if output_dir:
            os.environ['OUTPUT_DIR'] = output_dir

        # Run validation engine
        engine = ValidationEngine(config_file)
        summary = engine.run()

        # Display results
        click.echo()
        click.echo("=" * 80)
        click.echo(summary.get_summary_text())
        click.echo("=" * 80)

        # Exit with appropriate code
        exit_code = engine.get_exit_code(summary)

        if exit_code == 0:
            click.secho("✓ Validation completed successfully!", fg='green', bold=True)
        elif exit_code == 2:
            click.secho("⚠ Validation completed with warnings", fg='yellow', bold=True)
        else:
            click.secho("✗ Validation failed!", fg='red', bold=True)

        sys.exit(exit_code)

    except ValidationFrameworkError as e:
        click.secho(f"✗ Validation Error: {str(e)}", fg='red', err=True)
        sys.exit(1)
    except Exception as e:
        click.secho(f"✗ Unexpected Error: {str(e)}", fg='red', err=True)
        sys.exit(1)


@cli.command()
@click.argument('config_file', type=click.Path(exists=True))
def validate_config(config_file):
    """
    Validate configuration file without running validations.

    CONFIG_FILE: Path to YAML configuration file

    Example:
        python run_validation.py validate-config config/validation_config.yaml
    """
    try:
        click.echo(f"Validating configuration: {config_file}")

        loader = ConfigLoader(config_file)
        if loader.validate_only():
            click.secho("✓ Configuration is valid!", fg='green', bold=True)

            # Load and display summary
            config = loader.load()
            click.echo()
            click.echo(f"Connections:  {len(config.connections)}")
            click.echo(f"Validations:  {len(config.validations)}")
            click.echo(f"Reporters:    {len(config.reporters)}")

            sys.exit(0)
        else:
            click.secho("✗ Configuration is invalid!", fg='red', bold=True)
            sys.exit(1)

    except Exception as e:
        click.secho(f"✗ Configuration Error: {str(e)}", fg='red', err=True)
        sys.exit(1)


@cli.command()
def list_connectors():
    """
    List all supported connector types.

    Example:
        python run_validation.py list-connectors
    """
    from src.validation_framework.connectors.connector_factory import ConnectorFactory

    click.echo("Supported Connector Types:")
    click.echo()

    for connector_type in ConnectorFactory.get_supported_types():
        click.echo(f"  • {connector_type}")


@cli.command()
def list_validators():
    """
    List all supported validation types.

    Example:
        python run_validation.py list-validators
    """
    from src.validation_framework.validators.validator_factory import ValidatorFactory

    click.echo("Supported Validation Types:")
    click.echo()

    for validator_type in ValidatorFactory.get_supported_types():
        click.echo(f"  • {validator_type}")


@cli.command()
def list_reporters():
    """
    List all supported reporter types.

    Example:
        python run_validation.py list-reporters
    """
    from src.validation_framework.reporters.reporter_factory import ReporterFactory

    click.echo("Supported Reporter Types:")
    click.echo()

    for reporter_type in ReporterFactory.get_supported_types():
        click.echo(f"  • {reporter_type}")


@cli.command()
@click.argument('output_path', type=click.Path())
@click.option('--type', '-t', default='simple',
              type=click.Choice(['simple', 'full', 's3-to-hive'], case_sensitive=False),
              help='Template type to generate')
def generate_config(output_path, type):
    """
    Generate a sample configuration file.

    OUTPUT_PATH: Path where to save the generated config file

    Example:
        python run_validation.py generate-config config/my_config.yaml --type simple
    """
    import shutil

    try:
        # Determine source template
        project_root = Path(__file__).parent.parent
        template_map = {
            'simple': 'config/examples/simple_spark_validation.yaml',
            'full': 'config/validation_config.yaml',
            's3-to-hive': 'config/examples/s3_to_hive_validation.yaml'
        }

        template_path = project_root / template_map.get(type, template_map['simple'])

        if not template_path.exists():
            click.secho(f"✗ Template not found: {template_path}", fg='red')
            sys.exit(1)

        # Copy template to output path
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy(template_path, output_file)

        click.secho(f"✓ Configuration template generated: {output_file}", fg='green', bold=True)
        click.echo()
        click.echo(f"Edit the file to customize your validation configuration.")

    except Exception as e:
        click.secho(f"✗ Failed to generate config: {str(e)}", fg='red', err=True)
        sys.exit(1)


@cli.command()
def version():
    """Display framework version and information."""
    click.echo("Pre/Post Deployment Validation Framework")
    click.echo("Version: 1.0.0")
    click.echo()
    click.echo("Supported Platforms:")
    click.echo("  • Apache Spark")
    click.echo("  • Apache Hive")
    click.echo("  • AWS S3")
    click.echo("  • Azure ADLS")
    click.echo("  • Google Cloud Storage")


if __name__ == '__main__':
    cli()
