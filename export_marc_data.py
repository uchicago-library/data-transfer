#!/usr/bin/python3


import sys
from time import *
import datetime
from traceback import *
import argparse
import json
import uuid
import psycopg
from pprint import pprint, pformat
from functools import wraps
import logging
from lxml import etree
import re
import pendulum
from pathlib import Path
import pathlib
from functools import reduce
import tarfile
import subprocess
import pysftp
from pathlib import Path
import pendulum as pd


sys.argv = ['export_marc_data.py', '--server', 'gooseberry',
            '--job_type', 'export', '--data_set', 'incremental']


def get_args():
    p = argparse.ArgumentParser()
    p.add_argument('-s', '--server', action='store',
                   required=True, help='server_name')
    p.add_argument('-j', '--job_type', action='store', required=True,
                   help='One of export, import, or export/import')
    p.add_argument('-d', '--data_set', action='store', required=True,
                   help='One of incremental or full')
    p.add_argument('-g', '--log_file', action='store',
                   default='./export_marc_data.log',
                   help='File to which to log messages')
    args = p.parse_args()
    # l = [args.list, args.full, args.incremental]
    # if not len([i for i in l if i]) == 1:
    #     print("error: Arguemnts must include one of --list or --file")
    #     p.print_usage()
    #     sys.exit(1)
    return args


class main_class:
    def __init__(self):
        self.args = get_args()
        self.server = self.args.server
        self.job_type = self.args.job_type
        self.data_set = self.args.data_set
        self.pdb = None
        self.stack_trace = True
        self.log = None
        self.log_level = 'info'
        self.log_file = self.args.log_file
        self.record_pat = re.compile(r'<record>.+</record>', re.DOTALL)
        self.rt = re.compile('\x1d')
        self.sftp_server = 'sftp.folio-dev.indexdata.com'
        self.sftp_username = 'uchicago'
        self.sftp_password = 'xxx'
        self.export_path = 'marc_export'
        self.tar_files = []
        self.batch_size = 1000
        self.read_size = 3000000
        self.rmd = b''
        self.cur_batch = ''
        self.job = None
        self.files = []
        self.file = None
        self.rows = []
        self.marc_rows = []
        self.cur_time = pendulum.now('local')
        self.cols = {'job':  ['job_type', 'job_date', 'data_set'],
                     'file': ['job_id', 'tar_name', 'file_name',
                              'file_type', 'file_format', 'file_date'],
                     'marc': ['job_id', 'file_id', 'hrid', 'src',
                              'vu_dict', 'vu_del', 'vu_bin',
                              'pod_dict', 'pod_del', 'pod_bin']}
        self.status = {'eresource': None, 'etemp': None, 'oclccm': None,
                       'temp': None, 'retrocon': None, 'cat': None}


def main():
    ts = time()
    m = main_class()
    initialize_logging(m)
    m.pdb = pdb_class(m)
    m.log.info('Time started: %s' % ctime())
    try:
        init_db(m)
        process_job(m)
        process_files(m)
        process_recs(m)
    except Exception as e:
        m.log.error("Fatal error", exc_info=m.stack_trace)
    finally:
        m.pdb.cur.close()
        m.pdb.con.close()
        close_files(m)
        te = time()
        m.log.info('Time ended: %s' % ctime())
        m.log.info('Time taken: %2.3f secs' % (te-ts))
        return m


def process_job(m):
    create_job(m)
    save_job(m)
    

def create_job(m):
    m.job = job_dict(m, m.job_type, m.cur_time, m.data_set)


def save_job(m):
    cols = m.cols['job']
    row = [m.job[c] for c in cols]
    params = ','.join(['%s'] * len(cols))
    sql = f"insert into job ({','.join(cols)}) values ({params}) returning id"
    m.pdb.cur.execute(sql, row)
    m.job['id'] = m.pdb.cur.fetchone()[0]
    m.pdb.con.commit()


def process_files(m):
    fetch_tar_files(m)
    open_files(m)
    save_files(m)

    
def fetch_tar_files(m):
    p = Path('/home/arnt/dev/folio/export')
    for f in p.glob('*.tar.gz'): Path.unlink(f)
    with pysftp.Connection(host=m.sftp_server, username=m.sftp_username,
                           password = m.sftp_password) as sftp:
        with sftp.cd(m.export_path):
            l = sftp.listdir()
            l = [[re.search('\d+.+\d+', n).group(), n] \
                 for n in l if re.search('\d+.+\d+', n)]
            l = list(reversed(sorted(l)))
            l = [n[1] for n in l]
            if m.data_set in ['incremental']:
                acc = [next(n for n in l if n[:3] == 'inc')]
                if not acc:
                    s = "Incremental file not available on site"
                    raise Exception(s)
            if m.data_set in ['full']:
                acc = [n for n in l if re.search('full', n)]
                if not acc:
                    s = "Full export file not available on site"
                    raise Exception(s)
            m.tar_files = acc
            for n in m.tar_files: sftp.get(n)

def open_files(m):
    for tname in m.tar_files:
        tar = tarfile.open(tname, "r:gz")
        for tarinfo in tar:
            if tarinfo.isreg():
                fname = re.search('(/)(.+)', tarinfo.name).group(2)
                ftype = {'.mrc': 'marc', '.xml': 'marc',
                         '.txt': 'delete'}[fname[-4:]]
                ffmt = {'.mrc': 'binary', '.xml': 'xml',
                         '.txt': 'text'}[fname[-4:]]
                fdate = re.search('(\d+-\d+-\d+)',tname).group()
                fd = tar.extractfile(tarinfo.name)
                d = file_dict(None, m.job['id'], tname, fname, ftype,
                              ffmt, fdate, fd)
                m.files.append(d)

                                                         
def save_files(m):
    cols = m.cols['file']
    params = ','.join(['%s'] * len(cols))
    sql= f"insert into file ({','.join(cols)}) values ({params}) returning id"
    for d in m.files:
        row = [d[c] for c in cols]
        m.pdb.cur.execute(sql, row)
        d['id'] = m.pdb.cur.fetchone()[0]
    m.pdb.con.commit()


def close_files(m):
    for file in m.files: file['fd'].close()


def process_recs(m):
    for file in m.files:
        m.file = file
        if m.file['file_format'] in ['binary']:
            process_marc_recs(m)
        elif m.file['file_format'] in ['text']:
            process_delete_recs(m)
        elif m.file['file_format'] in ['xml']:
            process_marcxml_recs(m)


def process_marc_recs(m):
    m.rmd = b''
    while True:
        batch = m.rmd + m.file['fd'].read(m.read_size)
        m.cur_batch = batch
        m.rmd = b''
        if not batch: break
        fetch_recs(m)
        create_vufind_dict(m)
        create_vufind_bin(m)
        create_pod_dict(m)
        create_pod_bin(m)
        save_recs(m)
        m.marc_rows = m.rows

def fetch_recs(m):
    s = m.cur_batch
    d = m.file
    pos = s.rfind(b'\x1d')
    if pos != -1:
        m.rmd = s[pos+1:]
        s = s[:pos+1]
    s = s.decode('utf-8')
    l = re.split(m.rt, s)
    if l:
        if not l[-1]: l = l[:-1]
        l = [r + '\x1d' for r in l]
        l = [rec_dict(m, m.job['id'], d['id'], r) for r in l]
    if l: m.rows = l
        
    
def create_vufind_dict(m):
    for r in m.rows:
        if m.file['file_format'] in ['binary']:
            r['vu_dict'] = map_binary_to_dict(m, r['src'])
        elif m.file['file_format'] in ['xml']:
            r['vu_dict'] = map_xml_to_dict(m, r['src'])
        r['hrid'] = r['vu_dict']['fields'][0]['001']

        
def create_vufind_bin(m):
    for r in m.rows:
        r['vu_bin'] = map_dict_to_binary(m, r['vu_dict'])


def create_pod_dict(m):
    for r in m.rows:
        r['pod_dict'] = r['vu_dict']
        # filter_pod_records(m, r)
        filter_pod_fields(m, r)


def filter_pod_records(m, r):
    r['pod_incl'] = False
    for d in r['pod_dict']['fields']:
        if '929' in d:
            for s in d['subfields']:
                if 'a' in s:
                    if s['a'] in m.status:
                        r['pod_incl'] = True
    

def filter_pod_fields(m, r):
    l = r['pod_dict']['fields']
    keys = [list(d.keys())[0]for d in l]
    ht = {k: None for k in keys if k < '900' or k in ['927','928']}
    l = [d for d in l if list(d.keys())[0] in ht]
    r['pod_dict']['fields'] = l


def create_pod_bin(m):
    for r in m.rows:
        r['pod_bin'] = map_dict_to_binary(m, r['pod_dict'])


def save_recs(m):
    l = m.rows
    if not l: return
    for d in l:
        d['vu_dict'] = json.dumps(d['vu_dict'])
        d['pod_dict'] = json.dumps(d['pod_dict'] if d['pod_incl'] else None)
    cols = m.cols['marc']
    rows = [[d[c] for c in cols] for d in l]
    sql = f"copy marc ({','.join(cols)}) from STDIN"
    with m.pdb.cur.copy(sql) as copy:
        for r in rows: copy.write_row(r)
    m.pdb.con.commit()


def process_delete_recs(m):
    size = m.batch_size
    l = [int(r.strip()) for r in m.file['fd'].readlines()]
    batches = zip(range(0, len(l), size), range(size, len(l)+size, size))
    for beg, end in batches:
        m.cur_batch = l[beg:end]
        create_delete_recs(m)
        save_recs(m)

    
def create_delete_recs(m):
    l = m.cur_batch
    d = m.file
    l = [rec_dict(m, m.job['id'], d['id'], r) for r in l]
    for r in l:
        r['hrid'] = r['src']
        r['vu_del'] = r['src']
        r['pod_del'] = r['src']
    if l: m.rows = l


def process_marcxml_recs(m):
    s = m.file['fd'].read()
    s = s.decode('utf-8')
    l = re.findall(m.record_pat, s)
    m.cur_batch = l
    fetch_marcxml_recs(m)
    create_vufind_dict(m)
    create_vufind_bin(m)
    create_pod_dict(m)
    create_pod_bin(m)
    save_recs(m)


def fetch_marcxml_recs(m):
    l = m.cur_batch
    d = m.file
    if l: m.rows = [rec_dict(m, m.job['id'], d['id'], r) for r in l]
        
    
def map_xml_to_dict(m, r):
    xml = etree.fromstring(r)
    j = dict()
    for a in xml:
        if a.tag in ['leader']:
            j['leader'] = str(a.text)
            j['fields'] = []
        elif a.tag in ['controlfield']:
            tag, txt = (a.attrib['tag'], str(a.text))
            d = dict([[tag, txt]])
            j['fields'].append(d)
        elif a.tag in ['datafield']:
            tag, ind1, ind2 = (a.attrib['tag'],
                               a.attrib['ind1'], a.attrib['ind2'])
            d = dict([[tag, dict([['ind1', ind1],
                                  ['ind2', ind2], ['subfields', []]])]])
            for b in a:
                if b.tag in ['subfield']:
                    sf, txt = (b.attrib['code'], str(b.text))
                    subd = dict([[sf, txt]])
                    d[tag]['subfields'].append(subd)
            j['fields'].append(d)
    return j


def map_dict_to_json(m, d):
    return json.dumps(d, default=serialize_datatype)


def map_json_to_dict(m, d):
    return json.loads(d)


def map_binary_to_dict(m, r):
    st, ft, rt = ('\x1f', '\x1e', '\x1d')
    l = [s for s in re.split(ft, r[:-1]) if s]
    ldr, dir, flds = (l[0][0:24], l[0][24:], l[1:])
    tags = [dir[i:i+3] for i in range(0, len(flds)*12, 12)]
    j = dict([['leader', ldr], ['fields', []]])
    for k,v in zip(tags, flds):
        if k < '010':
            j['fields'].append(dict([[k, v]]))
        else:
            l = re.split(st, v)
            ind1, ind2 = list(l[0])
            d = dict([['ind1', ind1], ['ind2', ind2]])
            d['subfields'] = [{s[0]: s[1:]} for s in l[1:]]
            j['fields'].append(dict([[k, d]]))
    return j


def map_dict_to_binary(m, j):
    st, ft, rt = ('\x1f', '\x1e', '\x1d')
    ldr = j['leader']
    dir = []
    flds = []
    start_pos = 0
    for f in j['fields']:
        for t, v in f.items():
            if not isinstance(v, dict):
                fld = f"{v}{ft}"
            else:
                ind1 = v['ind1']
                ind2 = v['ind2']
                sfs = []
                for d in v['subfields']:
                    for k in d.keys():
                        sfs.append(f"{st}{k}{d[k]}")
                fld = (f"{ind1}{ind2}{''.join(sfs)}{ft}")
            dir.append(f"{t}{len(fld):0>4}{start_pos:0>5}")
            flds.append(fld)
            start_pos += len(fld)
    dir.append(ft)
    dir = ''.join(dir)
    flds.append(rt)
    flds = ''.join(flds)
    rec_length = f"{len(ldr)+len(dir)+len(flds):0>5}"
    dir_length = f"{(len(ldr)+len(dir)):0>5}"
    ldr = f"{rec_length}{ldr[5:12]}{dir_length}{ldr[17:]}"
    rec = f"{ldr}{dir}{flds}"
    return rec

    
def job_dict(m, jt, jd, ds):
    return {'id': 1,
            'job_type': jt, # import, export/import
            'job_date': jd,
            'data_set': ds} # incremental
            

def file_dict(m, jd, tn, fn, ft, ff, fdate, fd):
    return {'id': None,
            'job_id': jd,
            'tar_name': tn,
            'file_name': fn,
            'file_type': ft, # marc, delete
            'file_format': ff, # binary, text, xml
            'file_date': fdate,
            'fd': fd}


def rec_dict(m, job_id, file_id, src):
    return {'id': None,
            'job_id': job_id,
            'file_id': file_id,
            'hrid': None,
            'src': src,
            'vu_dict': None,
            'vu_del': None,
            'vu_bin': None,
            'pod_dict': None,
            'pod_del': None,
            'pod_bin': None,
            'pod_incl': True}


def init_db(m):
    sch = "diku_mod_inventory_storage"
    for tbl in ['marc', 'file', 'job']:
        sql = f"drop table if exists {sch}.{tbl}"
        m.pdb.cur.execute(sql)
        m.pdb.con.commit()
    sql = f"create table if not exists {sch}.job ( " \
        "id serial primary key, " \
        "job_type varchar(15) not null, " \
        "job_date timestamp with time zone not null, " \
        "data_set varchar(50) not null)"
    m.pdb.cur.execute(sql)
    m.pdb.con.commit()
    sql = f"create table if not exists {sch}.file ( " \
        "id serial primary key, " \
        "job_id int references job(id), " \
        "tar_name varchar(100) not null, " \
        "file_name varchar(100) not null, " \
        "file_type varchar(20) not null, " \
        "file_format varchar(20) not null, " \
        "file_date timestamp with time zone not null) "
    m.pdb.cur.execute(sql)
    m.pdb.con.commit()
    sql = f"create table if not exists {sch}.marc ( " \
        "id serial primary key, " \
        "job_id int references job(id), " \
        "file_id int references file(id), " \
        "hrid int, " \
        "src text, " \
        "vu_dict json, " \
        "vu_del int, " \
        "vu_bin text, " \
        "pod_dict json, " \
        "pod_del text, " \
        "pod_bin text, " \
        "constraint uc_hrid unique (hrid))"
    m.pdb.cur.execute(sql)
    m.pdb.con.commit()


class pdb_class:
    def __init__(self, m):
        self.dsn = "host='%s' dbname='folio' user='folio'" % m.server
        self.con = psycopg.connect(self.dsn)
        self.cur = self.con.cursor()


def initialize_logging(m):
    d = dict([['debug',logging.DEBUG],['info',logging.INFO],
              ['warning',logging.WARN],
              ['error',logging.ERROR],['critical',logging.CRITICAL]])
    logging.basicConfig(
        level=d[m.log_level],
        format = '%(asctime)s  %(name)-7s %(levelname)-6s %(message)s',
        datefmt = '%m-%d %H:%M',
        filename = m.log_file,
        filemode = 'w')
    m.log = logging.getLogger('loader')
    console = logging.StreamHandler()
    console.setLevel(d[m.log_level])
    formatter = logging.Formatter(
        '%(name)-6s:  %(levelname)-5s %(message)s')
    console.setFormatter(formatter)
    if not m.log.handlers: m.log.addHandler(console)


