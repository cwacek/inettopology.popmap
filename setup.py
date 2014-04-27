from setuptools import setup, find_packages

setup(
    name="inettopology.popmap",
    version="0.1",
    packages=find_packages(),
    zip_safe=False,

    install_requires=[
        "redis",
        "argparse",
    ],

    entry_points={
        'inettopology.modules': [
            'popmap = inettopology_popmap.cmdline'
        ],
    },

    package_data={
        'inettopology_popmap.resources': ['resources/*.dat']
    },

    author="Chris Wacek",
    author_email="cwacek@cs.georgetown.edu",
    description="Internet Topology Graph Creator, Point-of-Presence Module",
    license="LGPL"
)
