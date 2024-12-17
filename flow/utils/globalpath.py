import sys
from pathlib import Path
import getopt

def parent_path(filename):
    """

    :param filename:
    :return:
    """
    pth=filename
    path = Path.cwd()
    for x in path.parents:
        if x.parts[-1] == pth:
            if x in sys.path:
                print('sys existence')
                return x
            else:
                sys.path.append(x)
                print('sys append')
                return x

def project_name():
    opts,args = getopt.getopt(sys.argv[1:],'p')
    for op ,value in opts:
        if op == '-p':
            pth =value
        else:
            pth = 'assessment'
            print('使用默认名称')
        return pth