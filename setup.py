from setuptools import setup, find_packages

setup(
    name='sciunit-swarm',
    version='0.1.0',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'sciunit-swarm=sciunit_swarm.__main__:main',
        ],
    },
    install_requires=['sciunit2'],
    python_requires='>=3.6',
)
