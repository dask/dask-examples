import sys
import os
from pathlib import Path
import nbconvert
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import glob


def run_notebook(in_filepath):
    if not in_filepath.is_file():
        raise IOError('File "%s" not found.' % in_filepath)
    in_filepath = in_filepath.resolve()

    run_path = str(in_filepath.parent)

    nb = nbformat.read(in_filepath.open(), as_version=4)
    ep = ExecutePreprocessor(
        timeout=300, kernel_name='python%d' % sys.version_info[0])

    try:
        ep.preprocess(nb, {'metadata': {'path': run_path}})
    except Exception:
        msg = 'Error executing the notebook "%s".\n\n' % in_filepath
        print(msg)
        raise


print("Running Notebooks...")

for filename in glob.glob('../**/*.ipynb', recursive=True):
    print(filename)
    run_notebook(Path(filename))

print("Execution Complete!!!")
