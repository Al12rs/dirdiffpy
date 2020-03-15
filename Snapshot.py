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
from typing import Any, Callable, Dict, Generator, Iterator, List, Tuple
import xxhash
import os
import hashlib
import json
from globster import Globster
from logging import log

IndexerType = Tuple[str, Callable[[str], Any]]
IndexedDataType = Dict[str, Any]
IndexType = Dict[str, IndexedDataType]
DirSnapshotType = Dict[str, IndexType]


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
        return ("xxhash", cls.xxhash_file)

    @classmethod
    def GETMTIME(cls) -> IndexerType:
        return ("getmtime", os.path.getmtime)

    @classmethod
    def SHA256(cls) -> IndexerType:
        return ("sha256", cls.sha256_file)


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

    def __init__(self, directory=".", exclude_file: str = None,
                 excludes=['.git/', '.hg/', '.svn/']):

        if not os.path.isdir(directory):
            raise TypeError("Directory must be a directory.")
        self.directory = os.path.basename(directory)
        self.path = os.path.abspath(directory)
        self.parent = os.path.dirname(self.path)
        self.patterns = excludes
        self._files_cache: List[str] = []
        self._sub_dirs_cache: List[str] = []
        self._is_populated = False

        if exclude_file:
            self.exclude_file = os.path.join(self.path, exclude_file)
            if os.path.isfile(self.exclude_file):
                file_patt = filter(None,
                                   open(exclude_file)
                                   .read().split("\n"))
                self.patterns.extend(file_patt)

        self.globster = Globster(self.patterns)

    def is_excluded(self, path: str) -> bool:
        """ 
        Return whether 'path' is ignored based on exclude patterns
        """
        match = self.globster.match(self.relpath(path))
        if match:
            log.debug("{0} matched {1} for exclusion".format(path, match))
            return True
        return False

    def walk(self) -> Generator[Tuple[str, list[str], list[str]]]:
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

    def populate(self, force_refresh=False) -> None:
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

    def iterfiles(self, include_pattern: str = None,
                  abspath=False, force_refresh=False) -> Generator[str]:
        """ 
        Generator for all the files matching pattern and not already excluded.

        Uses cached file list if available.

        Args:
            pattern (str): Unix style (glob like/gitignore like) pattern
            abspath (bool): Whether to use absolute or relative (default) paths. 
            force_refresh (bool): Whether to refresh from disk or use the cache.
        """
        self.populate(force_refresh)

        globster = Globster([include_pattern])

        for f in self._files_cache:
            if include_pattern is None or globster.match(f):
                if abspath:
                    yield os.path.join(self.path, f)
                else:
                    yield f

    def itersubdirs(self, pattern:str=None, 
                    abspath=False, 
                    force_refresh=False) -> Generator[str]:
        """
        Generator for all subdirs matching pattern and not excluded.

        Uses cached dir list if available.

        Args:
            pattern (str): Unix style (glob like/gitignore like) pattern
            abspath (bool): whether to use absolute or relative (default) paths.
            force_refresh (bool): Whether to refresh from disk or use the cache.
        """
        self.populate(force_refresh)

        globster = Globster([pattern])

        for d in self._sub_dirs_cache:
            if pattern is None or globster.match(d):
                if abspath:
                    yield os.path.join(self.directory, d)
                else:
                    yield d

    def files(self, pattern:str=None,
              sort_key=lambda k: k,
              sort_reverse=False,
              abspath=False,
              force_refresh=False) -> List[str]:
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

    def subdirs(self, pattern:str=None, 
                sort_key=lambda k: k,
                sort_reverse=False, abspath=False,
                force_refresh=False) -> List[str]:
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

    def relpath(self, path: str) -> str:
        """ 
        Return a relative filepath to path from Dir path.
        """
        return os.path.relpath(path, start=self.path)

    def abspath(self, relpath: str) -> str:
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

    # def compress_to(self, archive_path=None):
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


def index_files(dir: Dir, file_idx_methods={}) -> dict:
    """
    Generate the files indexes using the idx_methods.
    
    Returns:
        files_index (dict): dictionary of relative file paths and 
            associated data: 
                Eg. relpath : {methodName : data, methodName : data}
    """
    files_index = {}
    for f in dir.iterfiles():
        files_index[f] = compute_file(dir, f, file_idx_methods)
    return files_index


def index_subdirs(dir: Dir, dir_idx_methods={}) -> dict:
    """
    Generate the directory indexes using the idx_methods.
    Returns:
        dirs_index (dict): dictionary of relative dir paths and 
            associated data: 
                Eg. relpath : {methodName : data, methodName : data}
    """
    dirs_index = {}
    for d in dir.itersubdirs():
        dirs_index[d] = compute_subdir(dir, d, dir_idx_methods)
    return dirs_index


def update_file_index(dir: Dir, file_index={},
                      file_idx_methods={},
                      files_to_update=None) -> dict:
    """
    Add additional data to files_index using file_idx_methods.

    If a file list is provided, only those files will be updated.

    If parsed files are missing from they index they are added with
    the data from file_idx_methods.

    Existing files in the index are not removed if missing.

    If data can't be computed a message is printed but the file
    entry is added anyways.

    Args:
        file_index (dict): dictionary to update.
        file_idx_methods (dict): name and function to apply to the files
            to obtain the new data.
        files_to_update (list): optional list of relative paths of files 
            on which to compute and add the new data. If missing 
            all indexed files are computed.
    """
    if files_to_update:
        files = files_to_update
    else:
        files = dir.files(force_refresh=True)
        dir.depopulate()
    for f in files:
        if not file_index.has_key(f):
            print("File was not in index: {}".format(f))
            file_index[f] = {}
        file_index[f].update(compute_file(dir, f, file_idx_methods))
    return file_index


def update_subdir_index(dir: Dir, subdirs_index={},
                        dir_idx_methods={},
                        dirs_to_update=None) -> dict:
    """
    Add additional data to subdirs_index using dir_idx_methods.
    If a dir list is provided, only those dirs will be updated.
    If parsed dirs are missing from they index they are added with
    the data from dir_idx_methods.
    Existing dirs in the index are not removed if missing.
    If data can't be computed a message is printed but the dir
    entry is added anyways.
    Args:
        subdirs_index (dict): dictionary to update. 
        dir_idx_methods (dict): name and function to apply to the dirs
            to obtain the new data.
        dirs_to_update (list): optional list of relative paths of dirs 
            on which to compute and add the new data. If missing 
            all indexed dirs are computed.
    """
    if dirs_to_update:
        dirs = dirs_to_update
    else:
        dirs = dir.subdirs(force_refresh=True)
        dir.depopulate()
    for d in dirs:
        if not subdirs_index.has_key(d):
            print("Dir was not in index: {}".format(d))
            subdirs_index[d] = {}
        subdirs_index[d].update(compute_subdir(dir, d, dir_idx_methods))
    return subdirs_index


def compute_file(dir: Dir, f_path, file_idx_methods) -> dict:
    """
    Compute data for a file using idx_methods.
    Args:
        f_path (str): relative path of the file.
    Returns:
        file_data (dict): dictionary of methodNames / generatedData
    """
    file_data = {}
    for method_key in file_idx_methods:
        idx_method = file_idx_methods[method_key]
        try:
            file_data[method_key] = idx_method(dir.abspath(f_path))
        except Exception as exc:
            print(f_path, exc)
    return file_data


def compute_subdir(dir: Dir, d_path, dir_idx_methods) -> dict:
    """
    Compute data for a subdirectory using idx_methods.
    Args:
        d_path (str): relative path of the subdir.
    Returns:
        dir_data (dict): dictionary of methodNames / generatedData
    """
    dir_data = {}
    for method_key in dir_idx_methods:
        idx_method = dir_idx_methods[method_key]
        try:
            dir_data[method_key] = idx_method(dir.abspath(d_path))
        except Exception as exc:
            print(d_path, exc)
    return dir_data


def snapshot_dir(targetDir: str,
                 excludes: List[str] =
                 ['.git/', '.hg/', '.svn/'],
                 file_indexers: List[IndexerType] =
                 [FileIndexers.XXHASH64()],
                 dir_indexers: List[IndexerType] = []) -> DirSnapshotType:
    """
    Return a snapshot dict of the passed dir path.

    Args:
        targetDir (str): Path of the target directory.
        excludes (list): List of gitignore like patters to exclude.
        file_indexers (list): list of name / function Tubles of indexing 
            operations to apply to the files in the folder.
            Eg: {"getmtime":os.path.getmtime, "sha256":filehash}
        dir_indexers (list): name / function Tuples of indexing 
            operations to apply to the subdirectories in the folder.
    """
    dir = Dir(targetDir, excludes=excludes)
    dir.populate(force_refresh=True)
    state = {}
    state['root'] = {dir.path: compute_subdir(dir, ".", dir_indexers)}
    state['subdirs'] = index_files(dir, dir_indexers)
    state['files'] = index_subdirs(dir, file_indexers)
    dir.depopulate()
    return state


def snapshot_to_json(snapshot: dict) -> str:
    """
    Return the json rappresentation of the passed snapshot

    Args:
        snapshot (dict): snapshot to serialize

    Returns:
        json_data (str): json serialized snapshot
    """
    return json.dumps(snapshot)


def json_to_snapshot(json_data: str) -> dict:
    """
    Return the snapshot parsed from passed json data.
    """
    return json.loads(json_data)


def json_file_to_snapshot(self, json_path: str) -> dict:
    """
    Return the state parsed from passed json file.
    """
    with open(json_path, 'r') as f:
        json_data = f.read()
    return json_to_snapshot(json_data)


def compare_entry(new_data: IndexedDataType,
                  old_data: IndexedDataType,
                  cmp_key: str = None) -> int:
    """
    Check modified status of an entry using common data keys 
    or cmp_key if set.

    Args:
        new_data (dict): new data organized in key / value
        old_data (dict): old data organized in key / value
        cmp_key (str): key of the value to use for the comparison,
            if None, all the common keys will be used instead.

    Returns:
        (int) with the following values:
             1: modified
             0: not modified
            -1: unknown 
    """
    if cmp_key:
        if (cmp_key in new_data
                and cmp_key in old_data):
            if new_data[cmp_key] != old_data[cmp_key]:
                # modified
                return 1
            else:
                # unknown
                return -1
    else:
        methods = old_data.keys() & new_data.keys()
        if len(methods) == 0:
            # unknown
            return -1
        else:
            for key in methods:
                if new_data[key] != old_data[key]:
                    # modified
                    return 1
    # not modified
    return 0


def compare_dir_snapshot(dir_snapshot_new: DirSnapshotType,
                         dir_snapshot_old: DirSnapshotType,
                         cmp_key: str = None) -> dict:
    """ 
    Compare `dir_snapshot_new' and `dir_snapshot_old' and return the diff.

    Args:
        dir_snapshot_new (dict): a DirSnapshot state
        dir_snapshot_old (dict): a DirSnapshot state
        cmp_key (str): name of the file index data 
            to use for the comparison. If missing all common 
            metadata will be used. If no common metadata is 
            found modified will be empty.

    Returns: 
        dict with the following keys:

            - deleted files `deleted`
            - created files `created`
            - modified files `modified`
            - unknown modified state `modified_unknown`
            - deleted directories `deleted_dirs`

    """
    old_files = dir_snapshot_old['files'].keys()
    new_files = dir_snapshot_new['files'].keys()
    old_dirs = dir_snapshot_old['subdirs'].keys()
    new_dirs = dir_snapshot_new['subdirs'].keys()

    data = {}
    data['deleted'] = list(old_files - new_files)
    data['created'] = list(new_files - old_files)
    data['modified'] = []
    data['modified_unknown'] = []
    data['deleted_dirs'] = list(old_dirs - new_dirs)

    for f in old_files & new_files:
        cmp_res = compare_entry(dir_snapshot_new['files'][f],
                                dir_snapshot_old['files'][f],
                                cmp_key)
        if cmp_res == 1:
            data['modified'].append(f)
        if cmp_res == 0:
            pass
        if cmp_res == -1:
            data['modified_unknown'].append(f)
    return data
