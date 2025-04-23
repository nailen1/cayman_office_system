from setuptools import setup, find_packages

setup(
    name="cayman_office_system",
    version="0.2.0",
    description="An integrated platform for offshore fund management, tailored for LIFE Asset Management.",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    author="June Young Park",
    author_email="your-email@example.com",
    url="https://github.com/nailen1/cayman_office_system",
    packages=find_packages(),
    include_package_data=True,
    install_requires=parse_requirements("requirements.txt"),
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "cayman_office_system = cayman_office_system.__main__:main"
        ]
    },
    package_data={
        # Include any data files here if needed
    },
    zip_safe=False,
)

def parse_requirements(filename):
    with open(filename, encoding="utf-8") as f:
        lines = f.readlines()
    return [line.strip() for line in lines if line.strip() and not line.startswith('#')]
