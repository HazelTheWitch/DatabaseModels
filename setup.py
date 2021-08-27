from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

description = (here / 'README.md').read_text(encoding='utf-8')

shortDescription = description.split('\n')[0]
longDescription = '\n'.join(description.split('\n')[1:])

setup(
    name='databasemodels',
    version='1.0.0',
    description=shortDescription,
    long_description=longDescription,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    python_requires='>=3.9',
    install_requires=[
        'psycopg',
        'pydantic'
    ]
)
