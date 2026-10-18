# Notes

`XAMS_amstrax_overview.pdf` — explainer of strax + amstrax: from redax chunks to `event_info`, where every plugin
enters the reconstruction stream, where the settings come from. Generated from this checkout (plugin names, versions,
inputs, files and line numbers parsed from the source; corrections from `amstrax_files`):

```bash
python docs/notes/make_amstrax_note.py            # needs matplotlib and reportlab; amstrax_files next to amstrax
```

Regenerate after a change to the plugins or contexts and commit the PDF and `figures/` with it. A new or removed
plugin stops the script until it gets a place and a description in the script.
