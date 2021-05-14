import os
import subprocess
import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
files = [os.path.join(BASE_DIR, f) for f in os.listdir(
    BASE_DIR)]  # list of the files in BASE directory


def _run(*args):
    '''core function'''
    try:
        return subprocess.check_call(['git'] + list(args))
    except:
        return 0

def commit():
    ''' commit function '''
    message = str(datetime.datetime.now()
                  )  # input("\nType in your commit message: ")
    commit_message = f'{message}'

    _run("commit", "-am", commit_message)
    _run("push", "--all" )


def _filter_on_size(size= 10000000, f=files):
    """core function to filter files to be added, take size in bytes"""
    files_list = [file for file in f if os.path.getsize(file) < size]

    return files_list


def add(size=0):
    if size == 0:
        _run("add", ".")
    else:
        files = _filter_on_size(size)
        _run("add", *files)


def main():
    print("adding files")
    add(size= 10**6 )  # change the number to filter files on size , size in bytes
    print('committing files')
    commit()
    print('done')


if __name__ == '__main__':
    main()
