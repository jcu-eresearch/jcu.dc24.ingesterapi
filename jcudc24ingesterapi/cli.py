#!/usr/bin/env python
# encoding: utf-8
'''
jcudc24ingesterapi.cli -- shortdesc

jcudc24ingesterapi.cli is a description

It defines classes_and_methods

@author:     user_name
        
@copyright:  2013 organization_name. All rights reserved.
        
@license:    license

@contact:    user_email
@deffield    updated: Updated
'''

import traceback
import sys
import os
import pprint
import json

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

from jcudc24ingesterapi.ingester_platform_api import IngesterPlatformAPI

__all__ = []
__version__ = 0.1
__date__ = '2013-01-23'
__updated__ = '2013-01-23'

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''
    
    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = globals()["__doc__"].split("\n")[1]
    program_license = '''%s

  Copyright 2013 James Cook University eResearch Centre. All rights reserved.
  
TODO License

USAGE
''' % (program_shortdesc)

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-s", "--server", dest="server", action="store", help="server connection information")
        parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]")
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        parser.add_argument(dest="verb", help="the action being performed", metavar="verb", nargs=1)
        parser.add_argument(dest="args", help="paths to folder(s) with source file(s) [default: %(default)s]", metavar="args", nargs='+')
        
        # Process arguments
        args = parser.parse_args()
        
        client = IngesterPlatformAPI(args.server, None)
        
        if args.verb[0] == "get":
            print "get"
        elif args.verb[0] == "search":
            pprint.pprint(client.search(args.args[0]))
        elif args.verb[0] == "post":
            obj = json.loads(args.args[1])
            obj["class"] = args.args[0]
            pprint.pprint(client._marshaller.obj_to_dict(client.post(client._marshaller.dict_to_obj(obj))))

        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception, e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_tb(exc_traceback, limit=1, file=sys.stderr)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == "__main__":
    sys.exit(main())