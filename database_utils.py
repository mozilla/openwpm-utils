# SQLite


def fetchiter(cursor, arraysize=10000):
    """Generator for cursor results"""
    while True:
        rows = cursor.fetchmany(arraysize)
        if rows == []:
            break
        for row in rows:
            yield row


def list_placeholder(length, is_pg=False):
    """Returns a (?,?,?,?...) string of the desired length"""
    return '(' + '?,'*(length-1) + '?)'


def optimize_db(cursor):
    """Set options to make sqlite more efficient on a high memory machine"""
    cursor.execute("PRAGMA cache_size = -%i" % (0.1 * 10**7))  # 10 GB
    # Store temp tables, indicies in memory
    cursor.execute("PRAGMA temp_store = 2")
