from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

description = (here / 'README.md').read_text(encoding='utf-8')

shortDescription = description.split('\n')[2]

setup(
    name='databasemodels',
    version='1.1.0',
    description=shortDescription,
    long_description=description,
    long_description_content_type='text/markdown',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    python_requires='>=3.9',
    install_requires=[
        'psycopg',
        'iso8601',
        'typing_extensions'
    ]
)
