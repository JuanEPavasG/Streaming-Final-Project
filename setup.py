from setuptools import setup, find_packages

setup(
    name="analyze_topcoins",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "google-cloud-bigquery",
        "python-json-logger",
        "pandas",
    ],
    entry_points={
        "console_scripts": [
            "analyze_topcoins=analyze_topcoins.analyze_topcoins:main",
        ],
    },
)
