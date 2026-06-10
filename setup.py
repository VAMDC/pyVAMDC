from setuptools import setup

setup(
    name="pyVAMDC",
    version="0.1",
    description="A brand-new library to interact with the VAMDC infrastructure",
    author="Carlo Maria Zwölf, Nicolas Moreau",
    packages=["pyVAMDC", "pyVAMDC.spectral", "pyVAMDC.radex"],
    package_dir={
        "pyVAMDC": ".",
        "pyVAMDC.spectral": "./spectral",
        "pyVAMDC.radex": "./radex",
    },
    package_data={"": ["./xsl/*.xsl"]},
    include_package_data=True,
    install_requires=[
        "pandas",
        "lxml",
        "numpy>=2.0.0",
        "requests",
        "rdkit",
        "duckdb>=0.9.0",
        "pyarrow>=10.0.0",
    ],
)
