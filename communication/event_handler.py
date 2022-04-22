from typing import Callable


class event_handler:
    def __init__(self, fn: Callable):
        self.fn = fn

    def __set_name__(self, owner, name):
        owner.handlers[self.fn.__name__] = self.fn
        setattr(owner, name, self.fn)
