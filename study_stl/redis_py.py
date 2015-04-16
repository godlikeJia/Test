import redis

conn = redis.Redis('192.168.159.128')
print(conn.get('key'))
