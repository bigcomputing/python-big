#!/usr/bin/env python
#
# Copyright (c) 2005-2008, REvolution Computing, Inc.
#
# NetWorkSpaces is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as published
# by the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
# USA
#

"""
Distutils installer for NetWorkSpaces
"""
import os, sys

if sys.version_info < (2, 2):
    print >> sys.stderr, "You must use at least Python 2.2 for NetWorkSpaces"
    sys.exit(1)

if os.environ.get('NWS_DOC_DIR'):
    docdir = os.environ['NWS_DOC_DIR']
elif hasattr(os, 'getuid') and os.getuid() == 0:
    docdir = '/usr/share/doc/nws-python'
else:
    docdir = 'nws-python'

if os.environ.get('NWS_MAN_DIR'):
    mandir = os.environ['NWS_MAN_DIR']
elif hasattr(os, 'getuid') and os.getuid() == 0:
    mandir = '/usr/share/man/man1'
else:
    mandir = 'nws-python'

exdir = os.path.join(docdir, 'examples')

ex_srcdir = os.environ.get('NWS_EX_SRCDIR', 'examples')
doc_srcdir = os.environ.get('NWS_DOC_SRCDIR', 'doc')
top_srcdir = os.environ.get('NWS_TOP_SRCDIR', '.')
man_srcdir = os.environ.get('NWS_MAN_SRCDIR', 'man')

ex_files = [os.path.join(ex_srcdir, x) for x in os.listdir(ex_srcdir) if not os.path.isdir(x)]
doc_files = [os.path.join(doc_srcdir, x) for x in os.listdir(doc_srcdir) if not os.path.isdir(x)]
doc_files += [os.path.join(top_srcdir, x) for x in ['README', 'README.Sleigh']]
man_files = [os.path.join(man_srcdir, x) for x in os.listdir(man_srcdir) if not os.path.isdir(x)]
if os.environ.get('NWS_WINDOWS', 'no') == 'yes':
    man_files = []

scripts = ['scripts/PythonNWSSleighWorker.py',
           'scripts/PythonNWSSleighWorker',
           'scripts/pybabelfish',
           'scripts/pybabelfishd']
if os.environ.get('NWS_WINDOWS', 'no') == 'yes':
    scripts += ['scripts/PyBabelfishService.py', 'scripts/PySleighService.py',
                'scripts/rwin.vbs']

from distutils import core
kw = {
    'name': 'nwsclient',
    'version': '1.6.4',
    'author': 'REvolution Computing, Inc.',
    'author_email': 'sbweston@users.sourceforge.net',
    'url': 'http://nws-py.sourceforge.net/',
    'license': 'GPL version 2 or later',
    'description': 'Python NetWorkSpaces',
    'packages': ['nws'],
    'scripts': scripts,
    'data_files': [
          (docdir, doc_files),
          (exdir, ex_files),
          (mandir, man_files),
    ],
    'platforms': ['any'],
    'long_description': """\
NetWorkSpaces (NWS) is a system that makes it very easy
for different scripts and programs running (potentially) on
different machines to communicate and coordinate with one
another.

The requirements for the NWS for Python are:

  Python 2.2 or later on Linux, Mac OS X, and other Unix systems.
  Python 2.4 or later on Windows.""",
}

if (hasattr(core, 'setup_keywords') and 'classifiers' in core.setup_keywords):
    kw['classifiers'] = [
        'Topic :: System :: Clustering',
        'Topic :: System :: Distributed Computing',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Intended Audience :: System Administrators',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Natural Language :: English',
    ]

core.setup(**kw)
