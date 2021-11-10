import os
import pathlib
 
 
def pytest_ignore_collect(path):
   root_dir = pathlib.Path(__file__).parent.resolve()
   scrap_dir = pathlib.PurePath(root_dir, 'mongodol', 'scrap')
   return str(scrap_dir) in str(path)