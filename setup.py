from setuptools import setup, find_packages

VERSION = '0.0.1'


setup(

    name='devranker',

    description='Rank Developers using Git metadata',

    version=VERSION,

    url='https://github.com/vvallab/devranker',

    download_url='',

    author='Vamsi Vallabhaneni',

    author_email='vamsikiran.v@gmail.com',

    packages=find_packages(),

    install_requires=[
        'click>=7.1.2',
        'pandas>=1.0.3',
        'PyDriller >=1.15.1',
        'elasticsearch >=7.0.0',
        'elasticsearch-dsl >=7.2.0'
        ],
 
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Version Control :: Git',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        ],

    include_package_data=True,

    entry_points={
        'console_scripts': ['devranker=devranker.cli:cli'],
    },
)
