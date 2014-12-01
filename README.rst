
Poopy - An unusable map-reduce engine over AMPQ
-----------------------------------------------

Install
^^^^^^^

**From PYPI**

#. ``pip install poopy`` (please use virtualenv)
#. Download this file
   https://bitbucket.org/leliel12/poopy/raw/tip/example/iris.arff?at=default
#. Download this file
   https://bitbucket.org/leliel12/poopy/raw/tip/example/randomforest.py
   and put in the same directory of *iris.arff*

**From REPO**

#. Install rabitMQ
#. Clone this repo
#. ``pip install -e .``
#. The archives *iris.arff* and *randomforest.py* are inside in ``examples/``

**Running**

#. Open two consoles (consoleA, consoleB)
#. In *consoleB* run ``poopy deploy ampq://localhost``
#. In *consoleA* execute
   ``poopy upload ampq://localhost path/to/iris.arff poopFS://iris.arff``
   now your file are uploaded to the "distributed file sistem"
#. In *consoleA* run
   ``poopy run ampq://localhost path/to/randomforest.py Script out``
#. Your output model are serialized in out/localtime


**Reading the model**

In python console

.. code-block:: python

    from poopy import serializers

    with open("out/file") as fp:
        model = serializers.load(fp)


TODO:
-----

- Implement correctly timeouts.
- Implement an error exchange.
- Real distributed file system.
- More than one map or reduce.
- More output formats than b64-pkl.
- Not use the central node as main memory.
- Reorder some modules into packages

