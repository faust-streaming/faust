from faust.tables.table import Table
from faust.types.tables import GlobalTableT


class GlobalTable(Table, GlobalTableT):
    """
    .. warning::
        Using a GlobalTable with multiple app instances may cause an
        app to be stuck in an infinite recovery loop. The current fix
        for this is to run the table with the following options::

            app.GlobalTable(..., partitions=1, recovery_buffer_size=1)
    """
