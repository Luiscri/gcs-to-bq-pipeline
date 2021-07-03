import setuptools

REQUIRED_PACKAGES = [
    'apache-beam',
    'apache-beam[gcp]'
]

setuptools.setup(
    name='ProcessRaw',
    version='0.0.1',
    description='Workflow to process raw entries.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    include_package_data=True
)