import io
import logging

import requests

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

DEFAULT_BUFFER_SIZE = 128 * 1024

START = 0
CURRENT = 1
END = 2
WHENCE_CHOICES = [START, CURRENT, END]

_HEADERS = {'Accept-Encoding': 'identity'}
"""The headers we send to the server with every HTTP request.

For now, we ask the server to send us the files as they are.
Sometimes, servers compress the file for more efficient transfer, in which case
the client (us) has to decompress them with the appropriate algorithm.
"""

def _clamp(value, minval, maxval):
    return max(min(value, maxval), minval)


class BufferedInputBase(io.BufferedIOBase):
    """
    Implement streamed reader from a web site.
    Supports Kerberos and Basic HTTP authentication.
    """

    def __init__(self, url, mode='r', buffer_size=DEFAULT_BUFFER_SIZE, 
                 kerberos=False, user=None, password=None):
        """
        If Kerberos is True, will attempt to use the local Kerberos credentials.
        Otherwise, will try to use "basic" HTTP authentication via username/password.

        If none of those are set, will connect unauthenticated.
        """
        if kerberos:
            import requests_kerberos
            auth = requests_kerberos.HTTPKerberosAuth()
        elif user is not None and password is not None:
            auth = (user, password)
        else:
            auth = None

        self.response = requests.get(url, auth=auth, stream=True, headers=_HEADERS)

        if not self.response.ok:
            self.response.raise_for_status()

        logger.debug('self.response: %r, raw: %r', self.response, self.response.raw)

        self.buffer_size = buffer_size
        self.mode = mode
        self._read_buffer = None
        self._read_iter = None

        self._current_pos = 0

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        """Flush and close this stream."""
        logger.debug("close: called")
        self.response = None

    def readable(self):
        """Return True if the stream can be read from."""
        return True

    def seekable(self):
        return False

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def read(self, size=-1):
        """
        Mimics the read call to a filehandle object.
        """
        if self.response is None:
            return ''

        if size == 0:
            return ''
        elif size < 0:
            retval = self.response.raw.read()
        else:
            if self._read_iter is None:
                self._read_iter = self.response.iter_content(self.buffer_size)
                self._read_buffer = b''

            while len(self._read_buffer) < size:
                try:
                    self._read_buffer += next(self._read_iter)
                except StopIteration:
                    # Oops, ran out of data early.
                    retval = self._read_buffer
                    self._current_pos += len(retval)
                    self._read_buffer = b''

                    return retval

            # If we got here, it means we have enough data in the buffer
            # to return to the caller.
            retval = self._read_buffer[:size]
            self._read_buffer = self._read_buffer[size:]

        self._current_pos += len(retval)
        return retval

    def read1(self, size=-1):
        """This is the same as read()."""
        return self.read(size=size)

    def readinto(self, b):
        """Read up to len(b) bytes into b, and return the number of bytes
        read."""
        data = self.read(len(b))
        if not data:
            return 0
        b[:len(data)] = data
        return len(data)


class SeekableBufferedInputBase(BufferedInputBase):
    """
    Implement seekable streamed reader from a web site.
    Supports Kerberos and Basic HTTP authentication.
    """

    def __init__(self, url, mode='r', buffer_size=DEFAULT_BUFFER_SIZE,
                 kerberos=False, user=None, password=None):
        """
        If Kerberos is True, will attempt to use the local Kerberos credentials.
        Otherwise, will try to use "basic" HTTP authentication via username/password.

        If none of those are set, will connect unauthenticated.
        """
        self.url = url

        if kerberos:
            import requests_kerberos
            self.auth = requests_kerberos.HTTPKerberosAuth()
        elif user is not None and password is not None:
            self.auth = (user, password)
        else:
            self.auth = None

        self.response = self._partial_request()

        if not self.response.ok:
            self.response.raise_for_status()

        logger.debug('self.response: %r, raw: %r', self.response, self.response.raw)

        self._current_pos = 0
        self._seekable = True

        self.content_length = int(self.response.headers.get("Content-Length", -1))
        if self.content_length < 0:
            self._seekable = False
        if self.response.headers.get("Accept-Ranges", "none").lower() != "bytes":
            self._seekable = False

        self.buffer_size = buffer_size
        self.mode = mode
        self._read_buffer = None
        self._read_iter = None

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    def seek(self, offset, whence=0):
        """Seek to the specified position.

        :param int offset: The offset in bytes.
        :param int whence: Where the offset is from.

        Returns the position after seeking."""
        logger.debug('seeking to offset: %r whence: %r', offset, whence)
        if whence not in WHENCE_CHOICES:
            raise ValueError('invalid whence, expected one of %r' % WHENCE_CHOICES)

        if not self.seekable():
            raise OSError

        if whence == START:
            new_pos = offset
        elif whence == CURRENT:
            new_pos = self._current_pos + offset
        elif whence == 2:
            new_pos = self.content_length + offset

        new_pos = _clamp(new_pos, 0, self.content_length)

        if self._current_pos != new_pos:
            self._current_pos = new_pos
            self._read_buffer = None
            self._read_iter = None
            response = self._partial_request(new_pos)
            if response.ok:
                self.response = response
            else:
                self.response = None

            logger.debug('new_position: %r', new_pos)

        return new_pos

    def tell(self):
        return self._current_pos

    def seekable(self, *args, **kwargs):
        return self._seekable

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    def _partial_request(self, start_pos=None):

        headers = _HEADERS.copy()

        if start_pos is not None:
            headers.update({"range": f"bytes={start_pos}-"})

        response = requests.get(self.url, auth=self.auth, stream=True, headers=headers)

        return response
