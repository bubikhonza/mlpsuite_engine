from setuptools import setup, find_packages

setup(
    name="mlpsuite_engine",
    author="bubikhonza@gmail.com",
    version="0.1",
    packages=["mlpsuite_engine"],
    package_dir={'mlpsuite_engine': 'src/mlpsuite_engine'},
    requires=["pyyaml"]
)
