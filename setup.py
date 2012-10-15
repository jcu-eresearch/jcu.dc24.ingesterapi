from setuptools import setup, find_packages

version = '1.0'

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
      packages=find_packages('src'),
      package_dir = {'': 'src'},
      namespace_packages=['jcu', 'jcu.dc24'],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'setuptools', "sqlalchemy",
          # -*- Extra requirements: -*-
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
