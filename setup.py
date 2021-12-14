from setuptools import setup, find_packages
import pathlib

from src.databasemodels import version

here = pathlib.Path(__file__).parent.resolve()

description = (here / 'README.md').read_text(encoding='utf-8')

shortDescription = description.split('\n')[2]

setup(
    name='databasemodels',
    version=str(version),
    description=shortDescription,
    long_description=description,
    long_description_content_type='text/markdown',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    python_requires='>=3.7, <4',
    install_requires=[
        'psycopg',
        'psycopg_binary',
        'iso8601',
        'typing_extensions',
    ],
    author='Hazel Rella',
    author_email='hazelrella11@gmail.com',
    url='https://github.com/HazelTheWitch/DatabaseModels'
)
