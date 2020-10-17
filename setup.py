import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

import pb2df

requirements = ["protobuf>=3.13.0", "pyspark>=3.0.0"]
setup_requirements = [
    "pytest-runner",
]
test_requirements = [
    "pytest>=6",
]


class PyTest(TestCommand):
    user_options = [("pytest-args=", "a", "Arguments to pass to py.test")]

    def initialize_options(self):
        super().initialize_options()
        self.pytest_args = []

    def finalize_options(self):
        super().finalize_options()
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest

        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


setup(
    name="pb2df",
    description="A python module for converting proto3 type/object to spark dafaframe object.",
    long_description=__doc__,
    version=pb2df.__version__,
    author=pb2df.__author__,
    author_email=pb2df.__email__,
    url="https://github.com/zhangxianbing/pb2df",
    packages=find_packages(),
    platforms="any",
    setup_requires=setup_requirements,
    install_requires=requirements,
    tests_require=test_requirements,
    cmdclass={"test": PyTest},
    test_suite="tests",
    zip_safe=False,
)