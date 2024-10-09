#!/bin/bash
# Pyflakes does not like the way we do __all__ += []. This simple script
# Changes all the files in amstrax to abide by this convention and
# removes the lines that have such a signature.
start="$(pwd)"
echo $start

cd amstrax
sed -e '/__all__ +=/ s/^#*/#/' -i ./*.py

cd $start
echo "done"
