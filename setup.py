import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

import pb2df


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
    version=pb2df.__version__,
    author=pb2df.__author__,
    author_email="",
    description="Convert ProtoBuf objects to Spark DataFrame.",
    long_description=__doc__,
    url="https://github.com/zhangxianbing/pb2df",
    packages=find_packages(),
    zip_safe=False,
    platforms="any",
    install_requires=["protobuf"],
    tests_require=["pytest"],
    cmdclass={"test": PyTest},
)