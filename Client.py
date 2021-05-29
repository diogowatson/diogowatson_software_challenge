def check_type(obj, type):
    if isinstance(obj, type):
        return obj
    else:
        raise AttributeError('obj ist not of type ' + type)


class Client:

    def __init__(self, name, middle_name, last_name, age):
        self.name = check_type(name, str)
        self.middle_name = check_type(middle_name, str)
        self.last_name = check_type(last_name, str)
        self.age = check_type(age, int)