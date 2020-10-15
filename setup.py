#
import os
import re

from setuptools import setup, find_packages


def read_version(fobj):
    # type: (file) -> str
    regex = re.compile(r'^__version__\s*=\s*u?[\'"]([^\'"]+)[\'"]')
    for line in fobj:
        matches = regex.match(line)
        if matches:
            return matches.group(1)

    # Else unknown version
    return '0.0'


HERE = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(HERE, 'pymongo_zipkin', '__init__.py')) as f:
    module_version = read_version(f)

with open(os.path.join(HERE, 'README.rst')) as f:
    README = f.read()

with open(os.path.join(HERE, 'CHANGES.rst')) as f:
    CHANGES = f.read()


setup(
    name='PyMongo-Zipkin',
    version=module_version,
    license='MIT',
    url='https://github.com/mklein0/pymongo-zipkin',
    author='Marcos Klein',
    author_email='mklein0@gmail.com',
    description='An zipkin extension for pymongo library based on py_zipkin.',
    long_description_content_type='text/x-rst',
    long_description=README + '\n\n' + CHANGES,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Monitoring',
    ],
    packages=find_packages(),
    platforms='any',
    install_requires=[
        'py_zipkin>=0.7.0',
        'PyMongo>=3.1',
    ],
)
