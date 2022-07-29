from pathlib import Path

import subprocess
from setuptools import setup, Distribution
from setuptools.command.build_ext import build_ext

HYADIS_PATH = Path(__file__).parent.absolute()


def fetch_readme():
    with open(HYADIS_PATH / "README.md", encoding='utf-8') as f:
        return f.read()


def fetch_requirements():
    with open(HYADIS_PATH / "requirements.txt", 'r') as fd:
        return [r.strip() for r in fd.readlines()]


def get_version():
    # TODO: Proper version info
    return '0.0.0'


class BuildExt(build_ext):

    def run(self):
        bazel_cmd = ["bazel"]
        bazel_flags = []
        bazel_targets = []
        subprocess.check_call(bazel_cmd + ["build"] + bazel_flags + ["--"] + bazel_targets)


class BinaryDistribution(Distribution):

    def has_ext_modules(self):
        return True


setup(
    name='HyaDIS',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'Topic :: System :: Distributed Computing',
    ],
    description='TO DO',
    long_description=fetch_readme(),
    long_description_content_type='text/markdown',
    python_requires='>=3.6',
    cmdclass={"build_ext": BuildExt},
    distclass=BinaryDistribution,
    install_requires=fetch_requirements(),
    entry_points={"console_scripts": ["hyadis=hyadis.scripts:main"]},
)
