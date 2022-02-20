def is_valid_data(eachdoc):
    if type(eachdoc) is list:
        if len(eachdoc) > 0:
            return True
        return False
    elif type(eachdoc) is str:
        if eachdoc and eachdoc != "" and len(eachdoc) > 0:
            return True
        return False
    elif type(eachdoc) is None:
        return False
    elif type(eachdoc) is dict:
        if len(eachdoc.keys()) > 0:
            return True
        return False
    elif type(eachdoc) is bool:
        return True
    elif type(eachdoc) is float or type(eachdoc) is int:
        return True
    else:
        return False
