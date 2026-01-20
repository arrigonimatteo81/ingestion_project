# -*- coding: utf-8 -*-


import os

from setuptools import setup, find_packages

install_requires = [
    # da installare quando parte il runtime
]

with open("README.rst") as f:
    readme = f.read()

with open("LICENSE") as f:
    license = f.read()

setup(
    name="ingestion_process",
    version=os.environ["VERSION"],
    description="Ingestion component",
    include_package_data=True,
    long_description=readme,
    author="Matteo Arrigoni",
    author_email="matteo.arrigoni81@gmail.com",
    license=license,
    packages=find_packages(exclude=("tests", "docs")),
    zip_safe=False,
    install_requires=install_requires
)
