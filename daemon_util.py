# Utilities for Python powered *nix daemons
#
# Copyright (C) 2015  Alexander A. Klimov
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import os
from os.path import realpath


__all__ = [
    'isProcessRunning', 'PIDFile',
    'PIDFileError', 'PIDFileNotFound', 'InvalidPIDFileContent', 'AlreadyRunning', 'PIDFileNotCreated'
]


def isProcessRunning(pid: int) -> bool:
    """
    Tell whether there's a process with the given PID.
    """

    try:
        os.getpgid(pid)
    except ProcessLookupError:
        return False
    return True


class PIDFileError(Exception):
    def __init__(self, *args, **kwargs):
        if type(self) is PIDFileError:
            raise NotImplementedError()

        super(PIDFileError, self).__init__(*args, **kwargs)


class PIDFileNotFound(PIDFileError):
    def __init__(self, *args, **kwargs):
        super(PIDFileNotFound, self).__init__("the PID file doesn't exist")


class InvalidPIDFileContent(PIDFileError):
    def __init__(self, content, *args, **kwargs):
        self.content = content
        super(InvalidPIDFileContent, self).__init__((
            "the PID file's content is invalid: " + ascii(content).lstrip('b')
        ) if content else 'the PID file is empty', *args, **kwargs)


class AlreadyRunning(PIDFileError):
    def __init__(self, pid, *args, **kwargs):
        self.pid = pid
        super(AlreadyRunning, self).__init__('the process is already running. PID: {}'.format(pid), *args, **kwargs)


class PIDFileNotCreated(PIDFileError):
    def __init__(self, *args, **kwargs):
        super(PIDFileNotCreated, self).__init__("the PIDFile hasn't been create()d")


class PIDFile:
    """Manages a PID file."""

    def __init__(self, path):
        """
        Initialize a PIDFile without doing anything with the file itself.
        (No visible side effects.)

        :param path: the PID file's path
        """

        self.path = realpath(path)
        self._file = None

    def getPID(self) -> int:
        """
        Get the PID from the file.

        :raise PIDFileNotFound: if the PID file doesn't exist
        :raise InvalidPIDFileContent: if the PID file doesn't contain a valid PID (not necessarily of a running process)
        """

        try:
            f = open(self.path, 'rb')
        except FileNotFoundError:
            raise PIDFileNotFound()

        with f:
            pidstr = f.read()

        if not pidstr.endswith(b'\n'):
            # Either the process has crashed or it's still writing to the PID file.
            raise InvalidPIDFileContent(pidstr)

        pid = pidstr.strip()
        if not pid or frozenset(pid) - frozenset(b'0123456789'):
            raise InvalidPIDFileContent(pidstr)

        pid = int(pid)
        if not pid:
            raise InvalidPIDFileContent(pidstr)

        return pid

    def create(self) -> bool:
        """
        Create the PID file on the file system. If it already exists, the process must not be running.

        :return: whether the file existed
        :raise AlreadyRunning: if the process is already running
        """

        existed = False

        while True:
            try:
                f = open(self.path, 'x', 1)
            except FileExistsError:
                try:
                    pid = self.getPID()
                except PIDFileNotFound:
                    pass
                else:
                    if isProcessRunning(pid):
                        raise AlreadyRunning(pid)

                    existed = True
                    self.remove()
            else:
                self.close()._file = f
                return existed

    def writePID(self, pid:int=None):
        """
        Write a PID to the PID file. Use the current process' ID if not given.

        :raise PIDFileNotCreated: if create() hasn't been called before
        """

        if self._file is None:
            raise PIDFileNotCreated()

        print(os.getpid() if pid is None else pid, file=self._file)
        return self

    def fileno(self) -> int:
        """
        Get the PID file's descriptor.

        :raise PIDFileNotCreated: if create() hasn't been called before
        """

        if self._file is None:
            raise PIDFileNotCreated()

        return self._file.fileno()

    def close(self):
        """
        Close the PID file if create() has been called before.
        """

        if self._file is not None:
            self._file.close()
            self._file = None

        return self

    def remove(self) -> bool:
        """
        Remove the PID file from the file system if it exists.

        :return: whether the file existed
        """

        try:
            os.remove(self.path)
        except FileNotFoundError:
            return False
        return True

    def __enter__(self):
        """
        Do nothing at all.
        """

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Call close() and remove().
        """

        self.close().remove()
