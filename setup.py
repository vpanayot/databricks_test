from setuptools import setup, find_packages

setup(
    name="dab_project",
    version="0.0.1",
    packages=find_pakcages(where="."),
    package_dir={"":"."},
    install_requires=["setuptools"]
)
