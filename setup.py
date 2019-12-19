"""
Overview of ``setup.py``
------------------------
This script is required for the ``pip install`` process to work.
"""

__version__ = "1.2.1"

from setuptools import setup, find_packages

long_description = f"""
postGIS_tools v{__version__} is a pip-installable helper module
that supports spatial data science work in PostgreSQL/PostGIS
"""

with open('requirements.txt') as f:
    requirements_lines = f.readlines()
install_requires = [r.strip() for r in requirements_lines]

pkgs = find_packages()

setup(
    name='postGIS_tools',
    version=__version__,
    author='Aaron Fraint, AICP',
    author_email='aaronfraint@altaplanning.com',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/aaronfraint/postGIS-tools.git',
    packages=pkgs,
    install_requires=install_requires,
)
