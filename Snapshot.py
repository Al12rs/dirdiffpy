"""Copyright (c) 2020 AL, hjk

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
from typing import Any, Callable, Dict, Iterator, List
import xxhash
import os
import hashlib
import json
from globster import Globster
from logging import log

IndexerType = Dict[str, Callable[[str], Any]]


class FileIndexers:
    """
    """

    @staticmethod
    def sha256_file(filepath: str, blocksize: int = 4096) -> str:
        """
        """
        sha = hashlib.sha256()
        with open(filepath, 'rb') as fp:
            while 1:
                data = fp.read(blocksize)
                if data:
                    sha.update(data)
                else:
                    break
        return sha.hexdigest()

    @staticmethod
    def xxhash_file(filepath: str, blocksize: int = 4096) -> str:
        xxhash64 = xxhash.xxh64()
        with open(filepath, 'rb') as fp:
            while 1:
                data = fp.read(blocksize)
                if data:
                    xxhash64.update(data)
                else:
                    break
        return xxhash64.hexdigest()

    @classmethod
    def XXHASH64(cls) -> IndexerType:
        return {"xxhash": cls.xxhash_file}

    @classmethod
    def GETMTIME(cls) -> IndexerType:
        return {"getmtime": os.path.getmtime}

    @classmethod
    def SHA256(cls) -> IndexerType:
        return {"sha256": cls.sha256_file}


class Dir(object):
    """
    Convenience directory wrapper.

    Allow exclusions of files,
    listing of contents,
    cashing of iteration,
    size,
    walking,
    hashing of files and entire folder,
    compressing.

    Args:
        directory (str): Path of the dir to wrap.
        exclude_file (str): Path to file containing exclusion pattern,
            None by default, you can also load .gitignore files.
        excludes (list): List of additional patterns for exclusion,
            by default: ['.git/', '.hg/', '.svn/']
    """

    def __init__(self, directory=".", exclude_file:str=None,
                 excludes=['.git/', '.hg/', '.svn/']):

        if not os.path.isdir(directory):
            raise TypeError("Directory must be a directory.")
        self.directory = os.path.basename(directory)
        self.path = os.path.abspath(directory)
        self.parent = os.path.dirname(self.path)
        self.patterns = excludes
        self._files_cache:List[str] = []
        self._sub_dirs_cache:List[str] = []
        self._is_populated = False

        if exclude_file:
            self.exclude_file = os.path.join(self.path, exclude_file)
            if os.path.isfile(self.exclude_file):
                file_patt = filter(None,
                                   open(exclude_file)
                                   .read().split("\n"))
                self.patterns.extend(file_patt)

        self.globster = Globster(self.patterns)

    def is_excluded(self, path:str) -> bool:
        """ 
        Return whether 'path' is ignored based on exclude patterns
        """
        match = self.globster.match(self.relpath(path))
        if match:
            log.debug("{0} matched {1} for exclusion".format(path, match))
            return True
        return False

    def walk(self) -> Iterator:
        """
        Walk the directory like os.path
        (yields a 3-tuple (dirpath, dirnames, filenames)
        except it exclude all files/directories on the fly. 
        """
        for root, dirs, files in os.walk(self.path, topdown=True):
            # TODO relative walk, recursive call if root excluder found???
            #root_excluder = get_root_excluder(root)
            ndirs = []
            # First we exclude directories
            for d in list(dirs):
                if self.is_excluded(os.path.join(root, d)):
                    dirs.remove(d)
                elif not os.path.islink(os.path.join(root, d)):
                    ndirs.append(d)

            nfiles = []
            for fpath in (os.path.join(root, f) for f in files):
                if not self.is_excluded(fpath) and not os.path.islink(fpath):
                    nfiles.append(os.path.relpath(fpath, root))

            yield root, ndirs, nfiles

    def populate_dir(self, force_refresh=False) -> None:
        """
        Walk the directory recursively and populate a cache of it's contents.

        Dir.patterns are used for exclusion.
        Paths are stored as relative to the Dir.path

        Args:
            force_refresh (bool): Whether to refresh from disk if cache
                                  is already populated.
        """
        if not force_refresh and self._is_populated:
            return

        self._files_cache.clear()
        self._sub_dirs_cache.clear()

        for root, dirs, files in self.walk():
            for f in files:
                relpath = self.relpath(os.path.join(root, f))
                self._files_cache.append(relpath)
            for d in dirs:
                relpath = self.relpath(os.path.join(root, d))
                self._sub_dirs_cache.append(relpath)

        self._is_populated = True

    def depopulate(self) -> None:
        """
        Clear the cached lists of files and folders, set depopulated state.
        """
        self._files_cache.clear()
        self._sub_dirs_cache.clear()
        self._is_populated = False

    def iterfiles(self, include_pattern:str=None, abspath=False, force_refresh=False):
        """ 
        Generator for all the files matching pattern and not already excluded.

        Uses cached file list if available.

        Args:
            pattern (str): Unix style (glob like/gitignore like) pattern
            abspath (bool): Whether to use absolute or relative (default) paths. 
            force_refresh (bool): Whether to refresh from disk or use the cache.
        """
        self.populate_dir(force_refresh)

        globster = Globster([include_pattern])

        for f in self._files_cache:
            if include_pattern is None or globster.match(f):
                if abspath:
                    yield os.path.join(self.path, f)
                else:
                    yield f

    def itersubdirs(self, pattern=None, abspath=False, force_refresh=False):
        """
        Generator for all subdirs matching pattern and not excluded.

        Uses cached dir list if available.

        Args:
            pattern (str): Unix style (glob like/gitignore like) pattern
            abspath (bool): whether to use absolute or relative (default) paths.
            force_refresh (bool): Whether to refresh from disk or use the cache.
        """
        self.populate_dir(force_refresh)

        globster = Globster([pattern])

        for d in self._sub_dirs_cache:
            if pattern is None or globster.match(d):
                if abspath:
                    yield os.path.join(self.directory, d)
                else:
                    yield d

    def files(self, pattern=None,
              sort_key=lambda k: k,
              sort_reverse=False,
              abspath=False,
              force_refresh=False) -> list:
        """
        Return a sorted list containing relative path of all files (recursively).

        Uses cached file list if available.

        Args:
            pattern (str): Unix style (glob like/gitignore like) pattern.
            sort_key (lambda): key argument for sorted
            sort_reverse (bool): reverse argument for sorted
            abspath (bool): whether to use absolute or relative (default) paths.
            force_refresh (bool): Whether to refresh from disk or use the cache.

        Return:
            List of all relative file paths.
        """
        return sorted(self.iterfiles(pattern, abspath, force_refresh),
                      key=sort_key,
                      reverse=sort_reverse)

    def subdirs(self, pattern=None, sort_key=lambda k: k,
                sort_reverse=False, abspath=False,
                force_refresh=False) -> list:
        """
        Return a sorted list containing relative path of all subdirs(recursively).

        Uses cached file list if available.

        Args:
            pattern (str): Unix style (glob like/gitignore like) pattern.
            sort_key (lambda): key argument for sorted
            sort_reverse (bool): reverse argument for sorted
            abspath (bool): whether to use absolute or relative (default) paths.
            force_refresh (bool): Whether to refresh from disk or use the cache.

        Return:
            List of all relative subdirs paths.
        """
        return sorted(self.itersubdirs(pattern, abspath, force_refresh),
                      key=sort_key,
                      reverse=sort_reverse)

    def relpath(self, path:str) -> str:
        """ 
        Return a relative filepath to path from Dir path.
        """
        return os.path.relpath(path, start=self.path)

    def abspath(self, relpath:str) -> str:
        """
        Return an absolute filepath from a relative to the root dir one.
        """
        return os.path.join(self.path, relpath)

    def size(self) -> int:
        """ 
        Return total directory size in bytes.

        Return:
            int: Total directory size in bytes.
        """
        dir_size = 0
        for f in self.iterfiles(abspath=True):
            dir_size += os.path.getsize(f)
        return dir_size

    #def compress_to(self, archive_path=None):
        """ Compress the directory with gzip using tarlib.

        :type archive_path: str
        :param archive_path: Path to the archive, if None, a tempfile is created

        """
    """    if archive_path is None:
            archive = tempfile.NamedTemporaryFile(delete=False)
            tar_args = []
            tar_kwargs = {'fileobj': archive}
            _return = archive.name
        else:
            tar_args = [archive_path]
            tar_kwargs = {}
            _return = archive_path
        tar_kwargs.update({'mode': 'w:gz'})
        with closing(tarfile.open(*tar_args, **tar_kwargs)) as tar:
            tar.add(self.path, arcname='', exclude=self.is_excluded)

        return _return
    """



def dirSnapshot(targetDir: str,
                excludes: List[str] =
                ['.git/', '.hg/', '.svn/'],
                file_indexers: List[IndexerType] =
                [FileIndexers.XXHASH64()],
                dir_indexers: List[IndexerType] = []):
    """
    """