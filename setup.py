from setuptools import setup, find_packages

setup(
    name='pyVAMDC',
    version='0.1',
    description='A brand-new library to interact with the VAMDC infrastructure',
    author='Carlo Maria Zwölf, Nicolas Moreau',
    packages=find_packages(),
    install_requires=['pandas','lxml'] 
)
