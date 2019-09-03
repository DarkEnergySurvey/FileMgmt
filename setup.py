import distutils
from distutils.core import setup
import glob

bin_files = glob.glob("bin/*")
etc_files = glob.glob("etc/*")

# The main call
setup(name='FileMgmt',
      version ='2.0.19',
      license = "GPL",
      description = "DESDM's file management framework",
      author = "Michelle Gower",
      author_email = "mgower@illinois.edu",
      packages = ['filemgmt'],
      package_dir = {'': 'python'},
      scripts = bin_files,
      data_files=[('ups',['ups/FileMgmt.table']),
                  ('etc',etc_files)]
      )

