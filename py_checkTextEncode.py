import chardet
from os import listdir


def get_txt_encode(filename):
    with open(filename, mode='rb') as f:
        return chardet.detect(f.read())['encoding']


path = 'E:/cclasdir/SY/out'
fns = (fn for fn in listdir(path=path) if fn.endswith('txt'))

for fn in fns:
    with open(fn) as fp:
        content = fp.read()
        encoding = chardet.detect(content)['encoding']
        print(encoding)
