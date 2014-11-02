
AMPoopQ - An unusable map-reduce engine over AMPQ
-------------------------------------------------

Install
^^^^^^^

#. Install rabitMQ
#. Clone this repo
#. ``pip install -r requirements.txt``
#. Open two consoles (consoleA, consoleB) in the same directory where you
   download AMPoopQ
#. In the two consoles execute ``export AMPOOPQ_URL=ampq://localhost``
#. In *consoleB* run ``python ampoopq.py deploy _node_dir``
   (*_node_dir* is the working directory for your deployed directory)
#. In *consoleA* execute
   ``python ampoopq.py upload empty.txt poopFS://empty.txt`` now your file
   are uploaded to the "fake distributed file sistem"
#. In *consoleA* run
   ``python ampoopq.py run imp.py _out --files poopFS://empty.txt``

In your directory if all work ok you will find a file with the text
"hello world" inside.

PS: probably nothing works
