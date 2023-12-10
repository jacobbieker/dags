from setuptools import find_packages, setup

setup(
    name="weather",
    packages=find_packages(exclude=["quickstart_etl_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "boto3",
        "pandas",
        "matplotlib",
        "monetio",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
