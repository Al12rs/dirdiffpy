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

import os
import json
import logging
import subprocess
from globster import Globster

log = logging.getLogger("dirstate")


class Dir(object):
    """
    Wrapper for dirstate for a path.

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

    def __init__(self, directory=".", exclude_file=None,
                 excludes=['.git/', '.hg/', '.svn/']):

        if not os.path.isdir(directory):
            raise TypeError("Directory must be a directory.")
        self.directory = os.path.basename(directory)
        self.path = os.path.abspath(directory)
        self.parent = os.path.dirname(self.path)
        self.patterns = excludes
        self._files_cache = []
        self._sub_dirs_cache = []
        self._is_populated = False

        if exclude_file:
            self.exclude_file = os.path.join(self.path, exclude_file)
            if os.path.isfile(self.exclude_file):
                file_patt = filter(None,
                                   open(exclude_file)
                                   .read().split("\n"))
                self.patterns.extend(file_patt)

        self.globster = Globster(self.patterns)

    def is_excluded(self, path) -> bool:
        """ 
        Return whether 'path' is ignored based on exclude patterns
        """
        match = self.globster.match(self.relpath(path))
        if match:
            log.debug("{0} matched {1} for exclusion".format(path, match))
            return True
        return False

    def walk(self) -> tuple:
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

    def iterfiles(self, include_pattern=None, abspath=False, force_refresh=False):
        """ 
        Generator for all the files matching pattern and not already excluded.

        Uses cached file list if available.

        Args:
            pattern (str): Unix style (glob like/gitignore like) pattern
            abspath (bool): Whether to use absolute or relative (default) paths. 
            force_refresh (bool): Whether to refresh from disk or use the cache.
        """
        self.populate_dir(force_refresh)

        if include_pattern is not None:
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

        if pattern is not None:
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

    def relpath(self, path) -> str:
        """ 
        Return a relative filepath to path from Dir path.
        """
        return os.path.relpath(path, start=self.path)

    def abspath(self, relpath) -> str:
        """
        Return an absolute filepath from a relative to the root dir one.
        """
        return os.path.join(self.dir.path, relpath)

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

    def compress_to(self, archive_path=None):
        """ Compress the directory with gzip using tarlib.

        :type archive_path: str
        :param archive_path: Path to the archive, if None, a tempfile is created

        """
        if archive_path is None:
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


class DirSnapshot(object):
    """
    Hold a snapshot of a directory, to be used for comparison.

    Attributes:
        dir (Dir): wrapper of the target directory with 
            convenience functions.
        file_idx_methods (dict[str, callable]): functions to apply 
            to obtain indexing data.
        dir_idx_methods (dict[str, callable]): functions to apply 
            to obtain indexing data.
        state (dict): contains the following keys:
                - root (dict[str, dict[str, Any]])
                - files (dict[str, dict[str, Any]])
                - subdirs (dict[str, dict[str, Any]])
            with each containing paths and dicts of named data.
    """

    def __init__(self, dir=None, state=None,
                 file_idx_methods={},
                 dir_idx_methods={}):
        """
        Args:
            dir (Dir): Wrapper object for the target directory.
            state (dict): 
            file_idx_methods (dict): name / function pairs of indexing 
                operations to apply to the files in the folder.
                Eg: {"getmtime":os.path.getmtime, "sha256":filehash}
            dir_idx_methods (dict): name / function pairs of indexing 
                operations to apply to the subdirectories in the folder.
        """
        self.dir = dir

        # self.idx_methods are here only for the user convenience to
        # keep track of the methods used, but are not actually used
        # internally. The user should care to update them if needed.
        self.file_idx_methods = file_idx_methods
        self.dir_idx_methods = dir_idx_methods

        self.state = state or self.compute_state(
            file_idx_methods, dir_idx_methods)

    def compute_state(self, file_idx_methods={}, dir_idx_methods={}) -> dict:
        """
        Calculate the snapshot of the folder.

        Does not set the internal state, nor update the idx_methods

        Returns:
            state: dictionary with the computed state.
                Structure: [str, [str, [str, Any]]]

        Example of state strcuture:
        ```json
        "state": {
            "root":{
                "root/path":{
                    "idx_method_name":"timeStamp",
                    "idx_method_name2":"size"
                }
            }
            "subdirs":{
                "dir/path1":{
                    "idx_method_name":"timeStamp",
                    "idx_method_name2":"size"
                },
                "dir/path2":{
                    "idx_method_name":"timeStamp",
                    "idx_method_name2":"size"
                }
            }
            "files":{
                "file/path":{
                    "idx_method_name":"hash",
                    "idx_method_name2":"hash2"
                },
                "file2/path2":{
                    "idx_method_name":"hash",
                    "idx_method_name2":"hash2"
                }
            }
        }
        ```
        """
        dir.populate(force_refresh=True)
        state = {}
        state['root'] = {self.dir.path: compute_subdir(".", dir_idx_methods)}
        state['subdirs'] = index_files(dir_idx_methods)
        state['files'] = index_subdirs(file_idx_methods)
        dir.depopulate()
        return state

    def index_files(self, file_idx_methods={}) -> dict:
        """
        Generate the files indexes using the idx_methods.

        Returns:
            files_index (dict): dictionary of relative file paths and 
                associated data: 
                    Eg. relpath : {methodName : data, methodName : data}
        """
        files_index = {}
        for f in self.dir.iterfiles():
            files_index[f] = self.compute_file(f, file_idx_methods)
        return files_index

    def index_subdirs(self, dir_idx_methods={}) -> dict:
        """
        Generate the directory indexes using the idx_methods.

        Returns:
            dirs_index (dict): dictionary of relative dir paths and 
                associated data: 
                    Eg. relpath : {methodName : data, methodName : data}
        """
        dirs_index = {}
        for d in self.dir.itersubdirs():
            dirs_index[d] = self.compute_subdir(d, dir_idx_methods)
        return dirs_index

    def update_file_index(self, file_index={},
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
            files = self.dir.files(force_refresh=True)
            self.dir.depopulate()
        for f in files:
            if not file_index.has_key(f):
                print("File was not in index: {}".format(f))
                file_index[f] = {}
            file_index[f].update(self.compute_file(f, file_idx_methods))
        return file_index

    def update_subdir_index(self, subdirs_index={},
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
            dirs = self.dir.subdirs(force_refresh=True)
            self.dir.depopulate()
        for d in dirs:
            if not subdirs_index.has_key(d):
                print("Dir was not in index: {}".format(d))
                subdirs_index[d] = {}
            subdirs_index[d].update(self.compute_subdir(d, dir_idx_methods))
        return subdirs_index

    def compute_file(self, f_path, file_idx_methods) -> dict:
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
                file_data[method_key] = idx_method(self.dir.abspath(f_path))
            except Exception as exc:
                print(f_path, exc)
        return file_data

    def compute_subdir(self, d_path, dir_idx_methods) -> dict:
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
                dir_data[method_key] = idx_method(self.dir.abspath(d_path))
            except Exception as exc:
                print(d_path, exc)
        return dir_data

    def to_json(self, state=None) -> str:
        """
        Return the json string of the internal state or the passed one if present.

        Args:
            state (dict): state to parse instead of self.state

        Returns:
            json_data (str): json serialized state
        """
        if state:
            state_to_parse = state
        else:
            state_to_parse = self.state
        return (json.dumps(state_to_parse))

    def from_json_data(self, json_data) -> dict:
        """
        Return the state parsed from passed json data.
        """
        state = json.loads(json_data)
        return state

    def from_json_file(self, json_path) -> dict:
        """
        Return the state parsed from passed json file.
        """
        with open(json_path, 'r') as f:
            json_data = f.read()
        return from_json_data(json_data)

    def __sub__(self, other) -> dict:
        """ 
        Compute diff with minus "-" operator overloading.

        Most commonly used for 
            recent_snapshot - older_snapshot.

        Returns: 
            dict with the following keys:
                - deleted files `deleted'
                - created files `created'
                - modified files `modified'
                - deleted directories `deleted_dirs'

        >>> snapshot_A = DirSnapshot(Dir('/path_A'))
        >>> snapshot_B = DirState(Dir('/path_B'))
        >>> diff =  snapshot_B - snapshot_A
        >>> # Equals to
        >>> diff = compute_diff(snapshot_B.state, snapshot_A.state)
        """
        return self.compute_diff(self.state, other.state)

    @classmethod
    def compute_diff(cls, dir_state_new, dir_state_old,
                     cmp_key=None) -> dict:
        """ 
        Compare `dir_state_new' and `dir_state_old' and return the diff.

        Args:
            dir_state_new (dict): a DirSnapshot state
            dir_state_old (dict): a DirSnapshot state
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
        old_files = dir_state_old['files'].keys()
        new_files = dir_state_new['files'].keys()
        old_dirs = dir_state_old['subdirs'].keys()
        new_dirs = dir_state_new['subdirs'].keys()

        data = {}
        data['deleted'] = list(old_files - new_files)
        data['created'] = list(new_files - old_files)
        data['modified'] = []
        data['modified_unknown'] = []
        data['deleted_dirs'] = list(old_dirs - new_dirs)

        for f in old_files.intersection(new_files):
            cmp_res = cls.compare_entry(new_files[f],
                                        old_files[f],
                                        cmp_key)
            if cmp_res == 1:
                data['modified'].append(f)
            if cmp_res == 0:
                pass
            if cmp_res == -1:
                data['modified_unknown'].append(f)
        return data

    @classmethod
    def compare_entry(cls, new_data, old_data, cmp_key=None) -> int:
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
            if (new_data.has_key(cmp_key)
                    and old_data.has_key(cmp_key)):
                if new_data[cmp_key] != old_data[cmp_key]:
                    # modified
                    return 1
                else:
                    # unknown
                    return -1
        else:
            methods = old_data.keys().intersection(new_data.keys())
            if methods.len() == 0:
                # unknown
                return -1
            else:
                for key in methods:
                    if new_data[key] != old_data[key]:
                        # modified
                        return 1
        # not modified
        return 0
