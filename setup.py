import setuptools

setuptools.setup(
    name="greenplum_admin",
    version="0.1.0",
    url="",

    author="Marcus Robb",
    author_email="marcus.robb@initworx.com",

    description="Admin scripts to help manage your cluster.",
    long_description=open('README.rst').read(),

    packages=setuptools.find_packages(),

    install_requires=['click>=6.7'
        ,'Sphinx>=1.7.2'
        ,'sphinx-click>=1.1.0'
        ,'sphinx-rtd-theme>=0.3.0'
        ,'sphinxcontrib-napoleon>=0.6.1'
        ,'sphinxcontrib-websupport>=1.0.1'
        ,'psycopg2-binary>=2.7.4'
    ],

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    entry_points={
        'console_scripts': [
        'greenplum_backup=greenplum_admin.greenplum_backup:main',
        ]
    },
)
