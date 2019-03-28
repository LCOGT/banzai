import importlib


def import_attribute(arg: str):
    module_name, attribute_name = arg.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, attribute_name)
