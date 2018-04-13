greenplum_admin package
=======================

greenplum_backup
----------------

This is a wrapper script for `gpcrondump`_.
The purpose is to manage rotation of backups automatically.

   - the script must be run from a master server in the cluster
   - the executing user must have superuser access to the cluster
   - the executing user must have write access to the back folder


.. click:: greenplum_admin.greenplum_backup:main
   :prog: greenplum_backup
   :show-nested:

.. automodule:: greenplum_admin.greenplum_backup
   :members:
   :undoc-members:
   :show-inheritance:


.. _`gpcrondump`: http://gpdb.docs.pivotal.io/550/utility_guide/admin_utilities/gpcrondump.html
