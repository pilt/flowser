from setuptools import setup

setup(name="flowser",
      version="0.1.0",
      description="High-level interface for Amazon Simple Workflow",
      author="Simon Pantzare",
      author_email="simon+flowser@pewpewlabs.com",
      url="https://github.com/pilt/flowser/",
      packages=["flowser"],
      license="MIT",
      platforms="Posix; MacOS X; Windows",
      classifiers = [
          "Development Status :: 3 - Alpha",
          "Topic :: Internet",
          "Intended Audience :: Developers",
          "License :: OSI Approved :: MIT License",
          "Operating System :: OS Independent",
          "Programming Language :: Python :: 2",
          "Programming Language :: Python :: 2.7",
          ],
      install_requires=[
          'boto>=2.4.1',
          ])
