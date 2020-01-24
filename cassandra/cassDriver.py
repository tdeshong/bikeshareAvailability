# pip install cassandra-driver
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()
session.execute("USE test")


session.execute(
    """
    INSERT INTO users (name, credits, user_id, username)
    VALUES (%(name)s, %(credits)s, %(user_id)s, %(name)s)
    """,
    {'name': "John O'Reilly", 'credits': 42, 'user_id': uuid.uuid1()}
)
