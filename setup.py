from setuptools import setup
from setuptools import find_packages
import sys

setup(name = "tornado-sqs",
      version = "1.0",
      description = "Asynchronous amazon SQS library using tornado",
      long_description = open("README").read(),
      author = "Clement Moussu",
      author_email = "clement@bittorrent.com",
      url = "https://github.com/moussu/tornado-sqs/",
      packages = find_packages(),
      license = "MIT",
      platforms = "Posix; MacOS X",
      )
