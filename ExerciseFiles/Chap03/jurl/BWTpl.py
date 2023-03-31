#!/usr/bin/python3
# bwTL - BW's template library
# by Bill Weinman [http://bw.org/]
# Copyright 1995-2021 The BearHeart Group LLC
# as of 2021-04-16

import re

__version__ = '0.7.3'
utf_8 = 'utf_8'


class TplString:
    """ string templating class """
    _vars = {}
    _sep = '\\$'
    flags = dict(
        showUnknowns=False,
        entityEncode=True
    )

    def __init__(self, s='', **kwargs):
        self._s = s
        self._init_re(kwargs)

    def _init_re(self, kwargs):
        if 'sep' in kwargs:
            self._sep = kwargs['sep']
        self._re = re.compile(r'{0}(.*?){0}'.format(self._sep))

    def _init_flags(self, kwargs):
        self.flags['showUnknowns'] = kwargs.get('showUnknowns', False)
        self.flags['entityEncode'] = kwargs.get('entityEncode', True)

    def var(self, k, v=None):
        if v is not None:
            self._vars[k] = str(v)
        if k in self._vars:
            return self._vars[k]
        elif self.flags['showUnknowns']:
            return f'** UNK {k} **'
        else:
            return None

    def parse(self, parse_str=None):
        s = self._s if parse_str is None else parse_str
        s = re.sub(self._re, self.replace, s)
        return s

    def replace(self, s):
        return self.var(s.group(1))


class TplFile(TplString):
    """ file templating """
    def __init__(self, fn, **kwargs):
        super().__init__('')
        self.__fh = open(fn, 'r', encoding=utf_8) if fn else None
        self._init_flags(kwargs)
        self._init_re(kwargs)

    def reset(self):
        self.__fh.seek(0)

    def file(self, fn):
        self.__fh = open(fn, 'r', encoding=utf_8)

    def readline(self):
        rl = self.__fh.readline()
        return self.parse(rl)

    def readlines(self):
        for rl in self.__fh.readlines():
            yield self.parse(rl)


def test():
    print('BWTpl.py version', __version__)
    x = 'This has a variable ($var$) and another (@two@) in it'

    st = TplString(x, sep='@')
    print('x is:', x)
    st.var('var', 'ONE')
    st.var('two', 'TWO')
    print(st.parse())

    fn = 'templatefile.txt'
    try:
        ft = TplFile(fn)
        ft.var('one', 'spam')
        ft.var('two', 'eggs')
        ft.var('three', 'ham')
        ft.var('four', 'rubber chicken')
        ft.var('five', '55555')
    
        print(str("ft.readline: " + ft.readline()).strip())
        
        for rl in ft.readlines():
            print(rl.strip())

        print('five is [{}]'.format(ft.var('five')))

    except IOError as e:
        print(f"Cannot open template file: {e}")


if __name__ == "__main__":
    test()
