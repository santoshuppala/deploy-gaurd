"""
Setup script for the Validation Framework.
"""
from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_file = Path(__file__).parent / 'README.md'
long_description = readme_file.read_text() if readme_file.exists() else ''

# Read requirements
requirements_file = Path(__file__).parent / 'requirements.txt'
requirements = []
if requirements_file.exists():
    with open(requirements_file) as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name='validation-framework',
    version='1.0.0',
    description='Pre/Post Deployment Validation Framework for Big Data',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Your Name',
    author_email='your.email@example.com',
    url='https://github.com/yourusername/validation-framework',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    install_requires=requirements,
    python_requires='>=3.8',
    entry_points={
        'console_scripts': [
            'validate=scripts.run_validation:cli',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Testing',
        'Topic :: Database',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    keywords='validation data-quality spark hive s3 bigdata etl',
    project_urls={
        'Documentation': 'https://github.com/yourusername/validation-framework/docs',
        'Source': 'https://github.com/yourusername/validation-framework',
        'Tracker': 'https://github.com/yourusername/validation-framework/issues',
    },
)
