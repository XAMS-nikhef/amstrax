================================
Amstrax - release procedure
================================

Making releases is essential to maintaining the code in order to do
this one needs to follow the following steps:
 - Check which changes where made (which pull requests are merged)
 - Update the ``HISTORY.md`` file
 - Go to the ``amstrax`` folder and increment the version with
   ``bumpversion patch`` (or ``bumpversion minor`` if there are a lot of fundamental changes)
 - Upload to master ``git push``
 - Add a tag ``git push --tags; git push``
 - Create a `new release <https://github.com/XAMS-nikhef/amstrax/releases>`_ by clicking ``Draft a new release``
 - There should be a tag that you just created, select it and copy past the code
   that you just wrote in the ``HISTORY.md``-file
 - If all goes well, the new version should become available on `PIPY <https://pypi.org/project/amstrax/>`_ in a few minutes
