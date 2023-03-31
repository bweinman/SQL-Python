#!/usr/bin/env python3
#
# db.py by Bill Weinman <https://bw.org/contact/>
# This is part of jurl - Jump to URL (a private short URL service)
# Copyright (c) 2010-2021 The BearHeart Group, LLC
# update 2021-04-16 - for SQL with Python

import sys
import os
import sqlite3

from hashlib import md5
from BWCGI import BWCGI
from BWDB import BWDB
from BWTpl import TplFile
from BWConfig import ConfigFile

__version__ = "3.1.1"

# namespace container for global variables
Gvars = dict(
    VERSION=f"db.py {__version__} BWDB {BWDB.version()}",
    template_ext='.html',
    table_name='jurl',
    config=ConfigFile('./jurl/db.conf').recs(),
    tpl=TplFile(None, showUnknowns=True),
    cgi=BWCGI(),
    db=BWDB(dbms='sqlite', database='./data/jurl.db'),
    stacks=dict(
        messages=[],
        errors=[],
        hiddens=[]
    )
)


def main():
    init()
    if 'a' in Gvars['vars']:
        dispatch()
    else:
        main_page()


def init():
    db = Gvars['db']
    Gvars['cgi'].send_header()
    Gvars['vars'] = Gvars['cgi'].vars()
    Gvars['linkback'] = Gvars['cgi'].linkback()

    # have table or create table
    if not db.have_table(Gvars['table_name']):
        if db.dbms == 'sqlite':
            create_sql = '''
                CREATE TABLE IF NOT EXISTS jurl (
                    id integer PRIMARY KEY,
                    shortURL VARCHAR(32) UNIQUE NOT NULL,
                    targetURL VARCHAR(128) NOT NULL
                )
            '''
        elif db.dbms == 'mysql':
            create_sql = '''
                CREATE TABLE IF NOT EXISTS jurl (
                    id integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    shortURL VARCHAR(32) UNIQUE NOT NULL,
                    targetURL VARCHAR(128) NOT NULL
                )
            '''
        else:
            create_sql = None
            error("invalid dbms")
        db.sql_do(create_sql)
    db.table = Gvars['table_name']


def dispatch():
    v = Gvars['vars']
    a = v.getfirst('a')
    if a == 'add':
        add()
    elif a == 'edit_del':
        if 'edit' in v:
            edit()
        elif 'delete' in v:
            delete_confirm()
        else:
            error("invalid edit_del")
    elif a == 'update':
        if 'cancel' in v:
            message('Edit canceled')
            main_page()
        else:
            update()
    elif a == 'delete_do':
        if 'cancel' in v:
            message('Delete canceled')
            main_page()
        else:
            delete_do()
    else:
        error("unhandled jump: ", a)
        main_page()


def main_page():
    # save values
    unkflag = Gvars['tpl'].flags['showUnknowns']
    Gvars['tpl'].flags['showUnknowns'] = False
    target_url = var('targetURL')
    short_url = var('shortURL')
    Gvars['tpl'].flags['showUnknowns'] = unkflag

    listrecs()

    if target_url is not None:
        var('targetURL', target_url)
    if short_url is not None:
        var('shortURL', short_url)

    hidden('a', 'add')
    page('main', 'Enter a new short URL')


def listrecs():
    """ display the database content """
    db = Gvars['db']
    v = Gvars['vars']
    sql_limit = int(Gvars['config'].get('sql_limit', 25))

    # how many records do we have?
    count = db.count_rows()
    message(f"There are {count or 'no'} records in the database. Add some more!")

    # how many pages do we have?
    numpages = count // int(sql_limit)
    if count % int(sql_limit):
        numpages += 1

    # what page is this?
    curpage = 0
    if 'jumppage' in v:
        curpage = int(v.getfirst('jumppage'))
    elif 'nextpage' in v:
        curpage = int(v.getfirst('pageno')) + 1
    elif 'prevpage' in v:
        curpage = int(v.getfirst('pageno')) - 1

    pagebar = list_pagebar(curpage, numpages)

    a = ''
    for r in db.get_rows_limit(sql_limit, curpage * sql_limit):
        dict_r = db.make_dict_row(r)
        set_form_vars(**dict_r)
        a += getpage('recline')

    set_form_vars()
    var('CONTENT', pagebar + a + pagebar)


def list_pagebar(pageno, numpages):
    """ return the html for the pager line """
    prevlink = '<span class="n">&lt;&lt;</span>'
    nextlink = '<span class="n">&gt;&gt;</span>'
    linkback = Gvars['linkback']

    if pageno > 0:
        prevlink = f'<a href="{linkback}?pageno={pageno}&prevpage=1">&lt;&lt;</a>'
    if pageno < (numpages - 1):
        nextlink = f'<a href="{linkback}?pageno={pageno}&nextpage=1">&gt;&gt;</a>'

    pagebar = ''
    for n in range(0, numpages):
        if n is pageno:
            pagebar += f'<span class="n">{n + 1}</span>'
        else:
            pagebar += f'<a href="{linkback}?jumppage={n}">{n+1}</a>'

    var('prevlink', prevlink)
    var('nextlink', nextlink)
    var('pagebar', pagebar)
    p = getpage('nextprev')
    return p


def page(pagename, title=''):
    """ display a page from html template """
    tl = Gvars['tpl']
    htmldir = Gvars['config']['htmlDir']
    file_ext = Gvars['template_ext']
    var('pageTitle', title)
    var('VERSION', Gvars['VERSION'])
    set_stack_vars()
    for p in ('header', pagename, 'footer'):
        try: 
            tl.file(os.path.join(htmldir, p + file_ext))
            for line in tl.readlines():
                print(line, end='')     # lines are already terminated
        except IOError as e:
            errorexit(f'Cannot open file ({e})')
    exit()


def getpage(p):
    """ return a page as text from an html template """
    tl = Gvars['tpl']
    htmldir = Gvars['config']['htmlDir']
    file_ext = Gvars['template_ext']
    a = ''
    try: 
        tl.file(os.path.join(htmldir, p + file_ext))
        for line in tl.readlines():
            a += line   # lines are already terminated
    except IOError as e:
        errorexit(f'Cannot open file ({e})')
    return a


# === actions
def add():
    db = Gvars['db']
    v = Gvars['vars']
    cgi = Gvars['cgi']

    if 'shortURL' in v:
        short_url = v.getfirst('shortURL')
    else:
        short_url = ''

    if 'targetURL' in v:
        target_url = v.getfirst('targetURL')
    else:
        target_url = ''
        main_page()

    rec = dict(
        shortURL=cgi.entity_encode(short_url),
        targetURL=cgi.entity_encode(target_url)
    )

    if 'generate' in v:
        rec['shortURL'] = shorten(target_url)
        set_form_vars(**rec)
        hidden('a', 'add')
        main_page()

    if 'shortURL' in v:
        try:
            db.add_row((rec['shortURL'], rec['targetURL']))
        except sqlite3.IntegrityError:
            error('Duplicate Short URL is not allowed')
            set_form_vars(**rec)
            hidden('a', 'add')
            main_page()
        message(f"Record ({rec['shortURL']}) added")

    main_page()


def edit():
    db = Gvars['db']
    row_id = Gvars['vars'].getfirst('id')
    rec = db.make_dict_row(db.get_row(row_id))
    set_form_vars(**rec)
    hidden('a', 'update')
    hidden('id', row_id)
    hidden('sURL', rec['shortURL'])
    page('edit', 'Edit this short URL')


def delete_confirm():
    db = Gvars['db']
    row_id = Gvars['vars'].getfirst('id')
    rec = db.make_dict_row(db.get_row(row_id))
    set_form_vars(**rec)
    hidden('a', 'delete_do')
    hidden('id', row_id)
    hidden('shortURL', rec['shortURL'])
    page('delconfirm', 'Delete this short URL?')


def delete_do():
    db = Gvars['db']
    v = Gvars['vars']

    row_id = v.getfirst('id')
    short_url = v.getfirst('shortURL')
    db.del_row(row_id)
    message(f'Record ({short_url}) deleted')
    main_page()


def update():
    db = Gvars['db']
    v = Gvars['vars']
    cgi = Gvars['cgi']

    short_url = cgi.entity_encode(v.getfirst('sURL'))
    row_id = v.getfirst('id')
    rec = dict(
        id=row_id,
        targetURL=cgi.entity_encode(v.getfirst('targetURL'))
    )
    db.update_row(row_id, rec)
    message(f'Record ({short_url}) updated')
    main_page()


# === manage template variables
def var(n, v=None):
    """ shortcut for setting a variable """
    return Gvars['tpl'].var(n, v)


def set_form_vars(**kwargs):
    s = kwargs.get('shortURL', '')
    t = kwargs.get('targetURL', '')
    row_id = kwargs.get('id', '')
    var('shortURL', s)
    var('targetURL', t)
    var('id', row_id)
    var('SELF', Gvars['linkback'])


def stackmessage(stack, *m_list, **kwargs):
    sep = kwargs.get('sep', ' ')
    m = sep.join(str(i) for i in m_list)
    Gvars['stacks'][stack].append(m)


def message(*m_list, **kwargs):
    stackmessage('messages', *m_list, **kwargs)


def error(*m_list, **kwargs):
    if 'cgi' in Gvars:
        stackmessage('errors', *m_list, **kwargs)
    else:
        errorexit(' '.join(m_list))


def hidden(n, v):
    Gvars['stacks']['hiddens'].append([n, v])


def set_stack_vars():
    a = ''
    for m in Gvars['stacks']['messages']:
        a += f'<p class="message">{m}</p>\n'
    var('MESSAGES', a)
    a = ''
    for m in Gvars['stacks']['errors']:
        a += f'<p class="error">{m}</p>\n'
    var('ERRORS', a)
    a = ''
    for m in Gvars['stacks']['hiddens']:
        a += f'<input type="hidden" name="{m[0]}" value="{m[1]}" />\n'
    var('hiddens', a)


# === utilities
def shorten(s):
    lookup = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    lsz = len(lookup)
    m = md5(s.encode('utf-8'))  # md5 because it's short - doesn't need to be secure or reversible
    out = m.digest()
    return ''.join('{}'.format(lookup[x % lsz]) for x in out)


def errorexit(e):
    me = os.path.basename(sys.argv[0])
    print('<p style="color:red">')
    print(f'{me}: {e}')
    print('</p>')
    exit(0)


def message_page(*m_list):
    message(*m_list)
    main_page()


def debug(*args):
    print(*args, file=sys.stderr)


if __name__ == "__main__":
    main()
