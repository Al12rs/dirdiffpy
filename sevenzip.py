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

import subprocess

class SevenZip(object):
    
    cls.exe_path = ''

    @classmethod
    def init(cls, exe_path):
        cls.exe_path = exe_path

    @classmethod
    def create_archive(cls, archive_path, dir_path):
        """
        Creates a new archive from dir_path files named archive_path
        """
        # option 'a' to use `add archive` option of 7z
        cmd = [cls.exe_path, 'a', archive_path, dir_path]
        subprocess.call(cmd)
        # if the management of stdout and stderr is needed, see subprocess.Popen()
    
    @classmethod
    def extract_archive(cls, archive_path, output_folder, file_list=[]):
        """
        Extracts files from the specified archivied to the folder indicated.
        """
        # option 'x' extract files with paths, -o{dir_path} specify output folder
        cmd = [cls.exe_path, 'x', archive_path, '-o{}'.format(output_folder)]
        # TO-DO: extract only files from a list. see include (-i{})
        subprocess.call(cmd)

    @classmethod
    def list_archive_contents(cls, archive_path, recurse=True):
        """
        List archive contents.

        Returns:
            List of files contained by the archive.
        """
        if recurse:
            cmd = [cls.exe_path, 'l', archive_path, "-r"]
        else:
            cmd = [cls.exe_path, 'l', archive_path]
        # assuming output is alright
        archive_contents = subprocess.check_output(cmd).decode('utf-8').splitlines()
        return archive_contents

    @classmethod
    def calculate_hash(cls, file_path, method="CRC32"):
        """
        Calculate hash of specified file.
        """
        cmd = [cls.exe_path, 'h', "-scrc{}".format(method), file_path]
        calculated_hash = subprocess.check_output(cmd).decode('utf-8').splitlines()
        return calculated_hash