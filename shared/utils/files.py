"""
File utility functions.
Provides functions for file operations (pickle, json, etc).
"""

import os
import shutil
from typing import List, Union, Any, Tuple

import joblib
from loguru import logger


def create_folder(path: str) -> None:
    """
    Create a folder if it doesn't exist.

    Args:
        path: Folder path to create
    """
    if not os.path.exists(path):
        os.makedirs(path)


def dump_files(objs: List[Any], path: str, files: List[str]) -> None:
    """
    Dump multiple objects to pickle files.

    Args:
        objs: List of objects to dump
        path: Directory path
        files: List of file names
    """
    path_files = [os.path.join(path, f) for f in files]
    for obj, file_path in zip(objs, path_files):
        joblib.dump(obj, file_path)
        logger.info(f"Dumped file: {file_path}")


def read_files(
    path: str,
    files: Union[List[str], str],
    read_all_directory: bool = False,
) -> Union[Tuple, Any]:
    """
    Read pickle files from a directory.

    Args:
        path: Directory path
        files: List of file names or single file name
        read_all_directory: If True, read all files in directory

    Returns:
        Tuple of loaded objects or single object
    """
    if read_all_directory:
        files = os.listdir(path)

    if isinstance(files, str):
        files = [files]

    path_files = [os.path.join(path, f) for f in files]
    results = tuple(map(joblib.load, path_files))

    if len(results) == 1:
        return results[0]

    return results


def copy_file(path_original: str, destination: str) -> None:
    """
    Copy a file to a destination.

    Args:
        path_original: Source file path
        destination: Destination path
    """
    shutil.copy(path_original, destination)


def delete_files_in_directory(path: str, exclude: List[str] = None) -> None:
    """
    Delete all files in a directory.

    Args:
        path: Directory path
        exclude: List of file/folder names to exclude
    """
    exclude = exclude or []
    files = os.listdir(path)

    for f in files:
        if f not in exclude:
            file_path = os.path.join(path, f)
            if os.path.isfile(file_path):
                os.remove(file_path)
