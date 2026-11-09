"""Write reference/datastructure_xams.rst: every data type of the default context amstrax.contexts.xams().

Per data type: the plugin (version, link to the source), its data kind, docstring, columns (st.data_info), a
dependency graph (graphviz; clickable) and every option that affects it (st.show_config). Called by conf.py at each
documentation build, so the page always matches the code; run by hand with ``python build_data_structure_doc.py``.
"""
import shutil
from pathlib import Path

import graphviz
import pandas as pd

import amstrax

HERE = Path(__file__).resolve().parent
OUT = HERE / "reference" / "datastructure_xams.rst"
GITHUB = "https://github.com/XAMS-nikhef/amstrax/blob/master"

HEADER = """
Data structure (amstrax {version})
============================================================

Every data type of the default context ``amstrax.contexts.xams()``: which plugin makes it, its columns, what it
depends on and which options affect it. Generated from the code at every build of this documentation.

The event level is four linked tables on the event windows plus the cut booleans: ``events``, ``sites``,
``event_peaks``, ``peak_context``, ``event_cuts`` (since amstrax 3.0; the old ``event_basics`` ... ``event_info`` of
amstrax 2.x are gone). How they link (``event_number``, ``peak_index``), definitions, worked examples and the map from
the old ``event_info`` columns: the XAMS handbook, Data processing → Event data format
(https://www.nikhef.nl/~xamsdata/processing/data-format/, Nikhef login), and the overview note
``docs/notes/XAMS_amstrax_overview.pdf`` (`on GitHub <{github}/docs/notes/XAMS_amstrax_overview.pdf>`_).

Colours in the graphs are data kinds: data types of the same kind have the same rows and can be loaded together in
one ``st.get_df``; different kinds need separate calls (join them by ``event_number`` / ``peak_index``).

Contents: {toc}

"""

TEMPLATE = """
.. _{anchor}:

{data_type}
--------------------------------------------------------------------

Plugin `{cls} <{github}/{module}.py>`_ v{version} · data kind ``{kind}`` · depends on {deps}

{docstring}

Columns
~~~~~~~

.. raw:: html

{columns}

Dependencies
~~~~~~~~~~~~

.. raw:: html

{svg}

Options
~~~~~~~

All options that affect this data type, also those of the plugins it depends on.

.. raw:: html

{config}

"""

KIND_COLORS = {
    "events": "#ffe9b3", "sites": "#ffd2a8", "event_peaks": "#d8f0c8",
    "peaks": "#a8e6a1", "records": "#ffc266", "raw_records": "#ff8a65", "pulse_counts": "#ffe0cc",
}
for _base in ("peaks", "records", "raw_records"):
    for _sfx in ("_ext", "_sipm"):
        KIND_COLORS[_base + _sfx] = KIND_COLORS[_base]

# order of the page: what an analysis reads first
FIRST = ["events", "event_cuts", "sites", "event_peaks", "peak_context", "event_windows",
         "peak_basics", "peak_positions", "peaks", "pulse_counts", "records", "raw_records"]


def indent(text):
    return "\n".join("    " + line for line in str(text).splitlines())


def order(st):
    rest = sorted(set(st._plugin_class_registry) - set(FIRST))
    return [d for d in FIRST if d in st._plugin_class_registry] + rest


def build_data_structure_doc():
    pd.set_option("display.max_colwidth", None)
    st = amstrax.contexts.xams(init_rundb=False, output_folder="/tmp/amstrax_docs_none")
    have_dot = shutil.which("dot") is not None
    types = order(st)
    toc = ", ".join(f":ref:`{d} <{d.replace('_', '-')}>`" for d in types)
    out = HEADER.format(version=amstrax.__version__, github=GITHUB, toc=toc)
    for data_type in types:
        plugins = st._get_plugins((data_type,), run_id="0")
        p = plugins[data_type]
        svg = "    <p>(graphviz not installed)</p>"
        if have_dot:
            g = graphviz.Digraph(format="svg")
            for d, q in plugins.items():
                g.node(d, style="filled", href="#" + d.replace("_", "-"),
                       fillcolor=KIND_COLORS.get(q.data_kind_for(d), "#dddddd"))
                for dep in q.depends_on:
                    g.edge(d, dep)
            body = g.pipe(format="svg").decode()
            svg = indent(body[body.index("<svg"):])
        cfg = st.show_config(data_type).sort_values(by="option")
        cfg["default"] = [x[:25] + "…" + x[-15:] if isinstance(x, str) and len(x) > 45 else x for x in cfg["default"]]
        doc = (p.__doc__ or "(no plugin description)").strip("\n")
        out += TEMPLATE.format(
            anchor=data_type.replace("_", "-"), data_type=data_type, cls=type(p).__name__, github=GITHUB,
            module=p.__module__.replace(".", "/"), version=getattr(p, "__version__", "?"),
            kind=p.data_kind_for(data_type),
            deps=", ".join(f"``{d}``" for d in p.depends_on) or "the redax live data",
            docstring="::\n\n" + indent(doc),
            columns=indent(st.data_info(data_type).to_html(index=False)),
            svg=svg, config=indent(cfg.to_html(index=False)))
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(out)
    print(f"written {OUT} ({len(types)} data types)")


if __name__ == "__main__":
    build_data_structure_doc()
