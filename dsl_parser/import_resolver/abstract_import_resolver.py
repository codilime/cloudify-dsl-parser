#########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.

import abc
import contextlib
import urllib2
import time
import requests

from dsl_parser import exceptions

DEFAULT_RETRY_DELAY = 1
DEFAULT_NUMBER_RETRIES = 5
DEFAULT_REQUEST_TIMEOUT = 30

class AbstractImportResolver(object):
    """
    This class is abstract and should be inherited by concrete
    implementations of import resolver.
    The only mandatory implementation is of resolve, which is expected
    to open the import url and return its data.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def resolve(self, import_url):
        raise NotImplementedError

    def fetch_import(self, import_url):
        url_parts = import_url.split(':')
        if url_parts[0] in ['http', 'https', 'ftp']:
            return self.resolve(import_url)
        return read_import(import_url)


def is_valid_code(code):
    if code >= 200 and code <=299:
        return True
    return False


def read_import(import_url):
    if import_url.startswith('file://'):
        try:
            with contextlib.closing(urllib2.urlopen(import_url)) as f:
                return f.read()
        except Exception, ex:
            ex = exceptions.DSLParsingLogicException(
                13, 'Import failed: Unable to open import url '
                    '{0}; {1}'.format(import_url, str(ex)))
            raise ex
    else:
        try:
            r = requests.get(import_url, timeout=DEFAULT_REQUEST_TIMEOUT)
            num_retries = 0
            while (not r.status_code == requests.codes.ok) and num_retries < DEFAULT_NUMBER_RETRIES:
                time.sleep(DEFAULT_RETRY_DELAY)
                r = requests.get(import_url)
                num_retries += 1
            if r.status_code == requests.codes.ok:
                return r.text
            else:
                ex = exceptions.DSLParsingLogicException(
                    13, 'Import failed: Unable to open import url '
                        '{0};'.format(import_url))
                raise ex
        except Exception, ex:
            ex = exceptions.DSLParsingLogicException(
                13, 'Import failed: Unable to open import url '
                    '{0}; {1}'.format(import_url, str(ex)))
            raise ex
