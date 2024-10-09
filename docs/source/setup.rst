========
Amstrax
========

Processing XAMS(L) data with ``amstrax``

Please notice that `amstrax` has two contexts:
 - ``amstrax.contexts.xams`` for XAMS
 - ``amstrax.contexts.xams_little`` for XAMSL data

We decided to use ``xams_little`` for XAMSL since it's otherwise too similar to ``xams``.


Setting up amstrax
===================

Installing `amstrax` is easy, just do:


.. code-block:: bash

    pip install amstrax


``amstrax`` is designed to be running on
`Nikhefs computing cluster <https://www.nikhef.nl/grid/computing-course/batch/stoomboot.html>`_
with an SSH-tunnel to the XAMS DAQ.

To ensure this tunnel to work, there are three requirements for environment
variables to be set. To ensure this is the case, please add the following
sniplet (with the passwords you will need to get from a colleague
to your ``.bashrc``-file.


.. code-block:: bash

    export DAQ_PASSWORD=<SECRET DAQ PASSWORD>
    export MONGO_USER=<MONGO DATABASE USER>
    export MONGO_PASSWORD=<MONGO DATABASE PASSWORD>


This additionally assumes you have have added your ``.ssh/id_rsa.pub``-key
on the DAQ machine (just add the output of ``cat .ssh/id_rsa.pub`` on
stoomboot to the ``.ssh/authorized_keys`` on the DAQ machine). If you don't
have an ssh key under ``.ssh/id_rsa.pub`` on stoomboot, google how to make one.

Straxen warnings
----------------
To make our live easier, we did not only include
`strax <https://github.com/AxFoundation/strax>`_ into the requirements
but also `straxen <https://github.com/XENONnT/straxen>`_. This might
give a few annoying warnings.

If you want to get rid of the utilix warning (which is totally
irrelevant for us, you can do ``touch ~/.xenon_config``).

