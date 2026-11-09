# Sphinx configuration of the amstrax documentation (https://amstrax.readthedocs.io).
#
# The reference pages are generated here, at every build, so they always match the code:
#   reference/amstrax*.rst          API reference from the docstrings (sphinx-apidoc)
#   reference/datastructure_xams.rst  every data type of the default context: plugin, columns, dependency graph,
#                                     options (build_data_structure_doc.py)
# Neither is kept in git (docs/source/.gitignore).
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))      # the amstrax checkout
sys.path.insert(0, str(HERE))

import amstrax  # noqa: E402

project = "amstrax"
author = "XAMS group, Nikhef"
copyright = "2020-2026, amstrax contributors and Nikhef"  # noqa: A001
version = release = amstrax.__version__

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
]
autodoc_default_options = {"members": True, "undoc-members": False, "show-inheritance": True}
autodoc_mock_imports = ["zstd", "blosc"]
intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

templates_path = []
exclude_patterns = ["_build", "**.ipynb_checkpoints"]
master_doc = "index"
language = "en"
pygments_style = "sphinx"
html_theme = "sphinx_rtd_theme"
html_static_path = []


def _generate_reference(app):
    """API pages and the data-structure page, written before Sphinx reads the sources."""
    ref = HERE / "reference"
    ref.mkdir(exist_ok=True)
    from sphinx.ext import apidoc
    apidoc.main(["--force", "--module-first", "--no-toc", "-o", str(ref), str(HERE.parents[1] / "amstrax"),
                 str(HERE.parents[1] / "amstrax" / "auto_processing_new")])
    if os.environ.get("AMSTRAX_DOCS_SKIP_DATASTRUCTURE"):
        return
    import build_data_structure_doc
    build_data_structure_doc.build_data_structure_doc()


def setup(app):
    app.connect("builder-inited", _generate_reference)
