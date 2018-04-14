from setuptools import setup, find_packages

setup(
    name='my own setup',
    version='1.0.0',
    description='setup the packages beside opentracing and adafruit',
    long_description='',
    author='DuyLd',
    license='',
    install_requires=[
                        'paho-mqtt', 'influxdb'
                     ],
    classifiers=[
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
    ],

    keywords=[ 'paho-mqtt' ],
)
