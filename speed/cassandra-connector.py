from cassandra.cluster import Cluster


# logging.basicConfig(level=logging.INFO)


def cassandra_connection():
    """
    Connection object for Cassandra
    :return: session, cluster
    """
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS climate
        WITH REPLICATION =
        { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }
        """)
    session.set_keyspace('climate')
    session.execute("""
        CREATE TABLE IF NOT EXISTS location_data (timestamp int, temperature float, wind float, humidity float, location text, weather text, PRIMARY KEY(location, timestamp))
        """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS trends_view (interval text, actual_temperature float, temperature_trend float, actual_wind float, wind_trend float, actual_humidity float, humidity_trend float, location text, weather text, PRIMARY KEY(location, interval))
        """)
    return session, cluster

session, cluster = cassandra_connection()

# session.execute('DROP KEYSPACE climate')

# print(cluster.metadata.keyspaces['climate'].tables)
# print(cluster.metadata.keyspaces['climate'].tables['trends_view'].columns)


# rows = session.execute("INSERT INTO trends_view (interval, actual_temperature, temperature_trend, actual_wind, wind_trend, actual_humidity, humidity_trend, location, weather) VALUES ('61', 12.1, 0.1, 20.0, -0.2, 23.0, 0.6, 'pro', 'wind')")

# rows = session.execute('SELECT interval, actual_temperature, temperature_trend, actual_wind, wind_trend, actual_humidity, humidity_trend, location, weather FROM trends_view ')
# for (interval, actual_temperature, temperature_trend, actual_wind, wind_trend, actual_humidity, humidity_trend, location, weather) in rows:
#     print(interval, actual_temperature, temperature_trend, actual_wind, wind_trend, actual_humidity, humidity_trend, location, weather)