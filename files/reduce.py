""" Reduce function template, enter your code below '### START ###' """


def emit(v: str) -> None:
    """
    Reduce stage will generate list of values:
        (k2,list(v2)) -> list(v2)
    """
    return None


key = ""  # key
values = ""  # values associated with key

### User code:
### START ###
result = sum(int(value) for value in values)
emit(str(result))
