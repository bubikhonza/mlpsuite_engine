import re


class Stage:
    def __init__(self, module_name: str, class_name: str, args: list) -> None:
        self.__module_name = module_name
        self.__class_name = class_name
        self.__args = self.__parse_args(args)

    def __parse_args(self, args_list: list) -> dict:
        d = {}
        for arg in args_list:
            key_val = arg.split("=")
            val_combined = key_val[1]
            val_type = re.search('\[(.*)\]', val_combined).group(1)
            val = re.search('(.*)\[', val_combined).group(1)
            d[key_val[0]] = eval(val_type)(val)
        return d

    def construct_pyspark_obj(self) -> object:
        import importlib
        class_ = getattr(importlib.import_module(self.__module_name), self.__class_name)
        instance = class_(**self.__args)
        return instance
