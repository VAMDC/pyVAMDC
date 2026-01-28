from setuptools import setup, find_packages

setup(
    name='pyVAMDC',
    version='0.1',
    description='A brand-new library to interact with the VAMDC infrastructure',
    author='Carlo Maria Zwölf, Nicolas Moreau',
    packages=["pyVAMDC", "pyVAMDC.spectral"],
    package_dir={
    'pyVAMDC': '.',
    'pyVAMDC.spectral': './spectral',
    },
    package_data={'':['./xsl/*.xsl']},
    include_package_data=True,
    install_requires=['pandas','lxml', 'numpy>=2.0.0', 'requests','rdkit', 'duckdb>=0.9.0', 'pyarrow>=10.0.0'] 
)
