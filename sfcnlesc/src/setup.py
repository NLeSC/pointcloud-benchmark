from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext
import os

src = ["pysfcnlesc.pyx", "sfcnlesc.cc"]    # list of source files
modules = [Extension("pysfcnlesc",
                    src,
                    language = "c++",
                    extra_compile_args=["-std=c++11", ],
                    extra_link_args=["-Lgeos_c", "-std=c++11"],
                    libraries=["geos_c"],)]

setup(name="pysfcnlesc",
     cmdclass={"build_ext": build_ext},
     ext_modules=modules,)

os.system('mv pysfcnlesc.so ../..')
