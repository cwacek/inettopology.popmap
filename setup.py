import distribute_setup
distribute_setup.use_setuptools()

from setuptools import setup, find_packages

setup(
    name="inettopology.popmap",
    version="0.1",
    packages=find_packages(),

    install_requires=[
        "redis",
        "argparse",
    ],

    entry_points={
        'inettopology.modules': [
            'popmap = inettopology_popmap.cmdline'
        ],
    },

    author="Chris Wacek",
    author_email="cwacek@cs.georgetown.edu",
    description="Internet Topology Graph Creator, Point-of-Presence Module",
    license="LGPL"
)
