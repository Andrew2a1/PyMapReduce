""" Map function template, enter your code below '### START ###' """


def emit(k: str, v: str) -> None:
    """
    Map stage will generate list of pairs:
        (k1,v1) -> list(k2,v2)
    """
    return None


key = ""  # document name
value = ""  # document contents

### User code:
### START ###
for v in value.split():
    emit(v, "1")
