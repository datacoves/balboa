from setuptools import find_packages, setup

setup(
    name="datacoves-airflow-provider",
    version="0.0.1",
    description="An Airflow provider for Datacoves",
    packages=find_packages(),
    install_requires=[
        "apache-airflow>=2.10",
    ],
    entry_points={
        "apache_airflow_provider": [
            "provider_info = datacoves_airflow_provider.provider_info:get_provider_info"
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
