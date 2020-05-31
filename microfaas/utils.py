class AsyncInit(type):
    """
    Metaclass to support the __ainit__() method.
    Note that calling this class will produce an awaitable for the instance, not
    the instance directly.
    """

    # TODO: Handle if __ainit__ is defined but not __init__

    async def __call__(cls, *pargs, **kwargs):
        self = super().__call__(*pargs, **kwargs)
        if hasattr(cls, '__ainit__'):
            await cls.__ainit__(self, *pargs, **kwargs)
        return self
