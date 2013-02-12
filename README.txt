1. Install python 2.7
2. Install easy_install + easy_install virtualenv
3. Setup git so that it is runnable from the command line - On windows:
	- Install msysgit 
	- Add to path variable as <installdir>\cmd
	- Install tortiosegit 
	- Ensure git works from the command line, cross your fingers..., if all else fails use a git-bash command prompt.
4. Create venv from python 2.7 (<python27>/Scripts/virtualenv --no-site-packages <location>)
5. Make sure the virtual env is configured with a valid c compiler - On windows this will probably involve:
	- Install mingw
	- Add <installdir>/bin and <installdir>/mingw32/bin to path
	- Add [build] compiler=mingw32 to venv/lib/distutils/distutils.cfg
	- Delete all -mno-cygwin within c:/python27/libs/distutils/cygwincompiler.py
6. Activate created virtual env
7. Checkout this git branch 
8. <venv>/python ./bootstrap.py - Use the fully qualified path - activate is buggy...
9. <venv>/python ./bin/buildout-script.py - Use the fully qualified path - activate is buggy...
