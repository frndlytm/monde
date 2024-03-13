from typing import Optional, Tuple, Union

import numpy as np
import pandas as pd
import pandera as pa
from pandas._typing import FilePath, WriteBuffer

from monde.utils import timed

Colspec = Tuple[Optional[int], Optional[int]]
PathOrBuffer = Union[FilePath, WriteBuffer[bytes], WriteBuffer[str]]


def empty(schema: pa.DataFrameSchema, size: int = 0) -> pd.DataFrame:
    index = list(schema.index or {})
    columns = index + list(schema.columns or {})
    data = np.empty((size, len(columns)))

    return (
        pd.DataFrame(data=data, columns=columns)
        .astype(
            {
                column_name: column_type.dtype.type.name
                for column_name, column_type in schema.columns.items()
            }
        )
        
    )


# See, https://pandas.pydata.org/docs/user_guide/io.html#insertion-method
# Alternative to_sql() *method* for DBs that support COPY FROM
@timed
def postgres_bulk_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
        table : pandas.io.sql.SQLTable
        conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
        keys : list of str Column names
        data_iter : Iterable that iterates the values to be inserted

    """
    # gets a DBAPI connection that can provide a cursor
    with conn.connection.cursor() as cursor:
        # Write the CSV to s string buffer to be used as COPY STDIN
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        # Quote the columns
        columns = ", ".join([f'"{k}"' for k in keys])
        table_name = f"{table.schema}.{table.name}" if table.schema else table.name

        # Build and execute the COPY statement
        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cursor.copy_expert(sql=sql, file=s_buf)
