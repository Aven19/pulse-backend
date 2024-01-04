"""Class for redis queue management"""


class RedisWorker:
    """Redis Queue Management worker"""

    @classmethod
    def clear_idle_redis_keys(cls):
        from app import logger
        from app import r
        """
        Clears idle keys from a Redis database based on their idle time.

        Args:
            redis_connection (redis.Redis): The Redis connection object.

        Note:
            This function iterates through all keys in the Redis database,
            checks their idle time, and deletes keys with an idle time
            greater than 604800 seconds (1 week).

        Example:
            To use this function with a Redis connection 'r', you can call:
            clear_idle_redis_keys(r)
        """

        try:
            logger.info('*' * 200)
            logger.info('Redis clear idle key...')
            keys_deleted = 0
            for key in r.scan_iter('*'):
                idle = r.object('idletime', key)   # type: ignore  # noqa: FKA100
                # logger.info(f'Key: {key}, Idle Time: {idle}')
                # idle time is in seconds. This is 168h
                if idle > 604800:
                    r.delete(key)
                    keys_deleted += 1
            logger.info(f'{keys_deleted} keys deleted.')
        except Exception as exception_error:
            logger.error('Unable to delete orphan jobs: '
                         + str(exception_error))
