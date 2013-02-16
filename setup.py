from setuptools import setup, find_packages

version = '1.0dev'

long_description = (
    open('README.txt').read()
    + '\n' +
    'Contributors\n'
    '============\n'
    + '\n' +
    open('CONTRIBUTORS.txt').read()
    + '\n' +
    open('CHANGES.txt').read()
    + '\n')

setup(name='jcu.dc24.ingesterapi',
      version=version,
      description="API for CC-DAM ingester platform",
      long_description=long_description,
      # Get more strings from
      # http://pypi.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[
        "Programming Language :: Python",
        ],
      author='Casey Bajema',
      author_email='casey@bajtech.com.au',
      url='http://www.bajtech.com.au',
      keywords='research data rda tdh ands web wsgi bfg pylons pyramid cc-dam ingester sensor',
      license='gpl',
      packages=find_packages('.'),
      package_dir = {'': '.'},
      namespace_packages=['jcudc24ingesterapi'],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'setuptools', "sqlalchemy", "simplesos"
          # -*- Extra requirements: -*-
      ],
      entry_points={
        "console_scripts": ["apiclient = jcudc24ingesterapi.cli:main"]
      },
      )
