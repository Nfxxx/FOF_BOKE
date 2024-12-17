class Pipeline(object):
    """Pipeline类的入口
        paramete：
            colums(dict):Pipeline的对象，可以支持多个
    """
    __slots__ = '_columns'
    def __init__(self,columns=None):
        if columns is None:
            columns={}
        self._columns = columns
    def colums(self):
        return self._columns
