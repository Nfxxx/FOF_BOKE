from os import path
from typing import List
import os


def get_abs_dir(file_path):
    """
    check input parameter is file path or not, if not ,change type to file path
    :param file_path:
    :return:
    """
    abs_path = path.abspath(file_path)
    if path.isdir(abs_path):
        return abs_path
    else:
        return path.dirname(abs_path)


def join_abs_dir(file_path , *relative):
    """
    Join file path
    :param file_path:
    :param relative:
    :return:
    """
    _dir = path.abspath(path.join(get_abs_dir(file_path),*relative))
    return _dir


def get_parent_dir(file_path):
    """
    get parent dir
    :param file_path:
    :return:
    """
    return  join_abs_dir(file_path,'..')


def get_proj_dir():
    """
    get project dir
    :return:
    """
    return join_abs_dir(__file__ , '..','..')

def find_models_packages():
    """
    get project dir
    :return:
    """
    from setuptools import find_packages
    models_path = join_abs_dir(os.path.dirname(os.path.dirname(__file__)))
    if not path.exists(models_path):
        raise Exception('model dir error')
    packages_list = find_packages(where=models_path,exclude=['*.*'])
    models_path = _check_packages_reachable(
        ["{package}".format(package = p) for p in packages_list]
    )

def _check_packages_reachable(package :List):
    """

    :param package:
    :return:
    """
    import importlib
    from flow.utils.exceptions import PlatformError
    filted = []
    for p in package :
        try:
            importlib.import_module(p)
        except PlatformError as e:
            print(e)
            continue
        filted.append(p)
    else:
        return filted
    