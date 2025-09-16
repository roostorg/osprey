from . import EtcdClient

if __name__ == '__main__':
    EtcdClient().delete('/doctest', directory=True, recursive=True)
    import doctest

    doctest.testmod()
