#!/usr/bin/env python
"""OnedataFSSpec is an fsspec filesystem implementation for Onedata."""

from setuptools import setup

__version__ = "25.0.0"

CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Programming Language :: Python :: 3.14",
    "Topic :: System :: Filesystems",
]

with open("requirements.txt", "rt", encoding="utf-8") as f:
    REQUIREMENTS = [
        line.strip()
        for line in f.readlines()
        if line.strip() and not line.startswith("#")
    ]

REQUIREMENTS_DEV = ["pytest>=6.0.0", "pytest-cov>=2.0.0", "pytest-mock>=3.0.0"]

setup(
    name="onedatafsspec",
    author="Bartek Kryza",
    author_email="bkryza@gmail.com",
    classifiers=CLASSIFIERS,
    description="Onedata filesystem implementation for fsspec",
    python_requires=">=3.10",
    install_requires=REQUIREMENTS,
    extras_require={"dev": REQUIREMENTS_DEV},
    license="MIT",
    long_description="An fsspec filesystem implementation for Onedata using onedatafilerestclient",
    packages=["onedatafsspec"],
    package_data={"onedatafsspec": ["*.txt", "*.md"]},
    include_package_data=True,
    keywords=["fsspec", "Onedata", "filesystem"],
    url="https://github.com/onedata/onedatafsspec",
    version=__version__,
    entry_points={"fsspec.specs": ["onedata=onedatafsspec.core:OnedataFileSystem"]},
)
