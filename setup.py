from setuptools import find_packages, setup

setup(
    name='dbmanager',
    packages=find_packages(include=['dbmanager']),
    version='0.1.0',
    description='A database manager that makes sqlalchemy easier',
    author='jingming@ualberta.ca',
    license='Apache',
    install_requires=[
        'sqlalchemy'
    ]
)
