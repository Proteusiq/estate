import setuptools

setuptools.setup(
    name="estates",
    packages=setuptools.find_packages(exclude=["estates_tests"]),
    install_requires=[
        "dagster==1.5.11",
        "dagit==0.14.16",
        "pytest",
    ],
)
