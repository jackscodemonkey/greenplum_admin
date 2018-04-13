greenplum_admin
===============

Admin scripts to help manage your cluster.


Installation
------------

Create a new virtual environment::

    $ virtualenv -p python2 greenplum_admin

Activate the new environment::

    $ source greenplum_admin/bin/activate

Your command prompt should now have the environment prefix showing::

   $ (greenplum_admin):

Quickly install greenplum_admin scripts into your new environment via `setuptools`_
Change to the directory where you downloaded this package and run::

   $ (greenplum_admin)python setup.py install


Requirements
^^^^^^^^^^^^

.. include:: ../../requirements.txt

Compatibility
-------------

These admin scripts are written in Python 2.7 to conform with the
current release of Greenplum 5.5

Licence
-------

Authors
-------

`greenplum_admin` was written by `Marcus Robb <marcus.robb@initworx.com>`_.


.. _`setuptools`: http://pypi.python.org/pypi/setuptools
