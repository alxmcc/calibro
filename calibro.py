import requests
import sqlite3
import json
import sys
from urllib.request import pathname2url
import re
import os
import time


class CalibreServer:
    def __init__(self, server_url):
        self.server_url = server_url  # add check to ensure 'http://' and no trailing '/'
        self.library_info, self.total_books = self.__library_book_count()
        self.server_info = json.dumps({'total_books': self.total_books, 'libraries': self.library_info})

    def get_book_ids(self, library, remaining, offset):
        try:
            r = requests.get(self.server_url + '/ajax/search/' + library + '?num='
                             + str(remaining) + '&offset=' + str(offset)
                             + '&sort=timestamp&sort_order=desc', verify=False)
        except requests.exceptions.RequestException as e:
            print(e)
            sys.exit(1)
        return ','.join(str(i) for i in r.json()['book_ids'])

    def get_book_ids_metadata(self, library, book_ids):
        try:
            r = requests.get(self.server_url + '/ajax/books/' + library + '?ids=' + book_ids, verify=False)
        except requests.exceptions.RequestException as e:
            print(e)
            sys.exit(1)
        return r.json()

    def __libraries(self):
        library_list = []
        try:
            r = requests.get(self.server_url + '/ajax/library-info', verify=False)  # need error checking
            for k, v in r.json()['library_map'].items():
                library_list.append(k)
        except requests.exceptions.RequestException as e:
            print(e)
            sys.exit(1)
        return library_list

    def __library_book_count(self):
        library_info = {}
        total_count = 0
        for library in self.__libraries():
            library_info[library] = {}
            try:
                r = requests.get(self.server_url + '/ajax/search/' + library + '?num=0', verify=False)
                library_info[library]['total_num'] = r.json()['total_num']
                library_info[library]['last_indexed'] = 0
                total_count += r.json()['total_num']
            except requests.exceptions.RequestException as e:
                print(e)
                sys.exit(1)
        return library_info, total_count


class Database(object):
    def __init__(self, db_file):
        try:
            dburi = 'file:{}?mode=rw'.format(pathname2url(db_file))
            self.connection = sqlite3.connect(dburi, uri=True)
            self.cursor = self.connection.cursor()
        except sqlite3.OperationalError:
            print('Database does not exist. Creating.')
            self.connection = sqlite3.connect(db_file)
            self.cursor = self.connection.cursor()
            self.cursor.execute("create table calibre (uuid varchar primary key, metadata json, "
                                "server varchar, library varchar)")
            self.cursor.execute("create table calibre_servers (server varchar primary key, metadata json)")
            self.connection.commit()

    def add_server_metadata(self, calibre_server):
        if self.__server_exists(calibre_server):
            print('Server already in DB. Skipped adding server metadata to DB')
        else:
            self.cursor.execute("insert into calibre_servers values (?, ?)",
                                [calibre_server.server_url, calibre_server.server_info])
            self.commit()

    def update_server_metadata(self, calibre_server):
        self.cursor.execute("update calibre_servers SET metadata=? where server=?",
                            [calibre_server.server_info, calibre_server.server_url])

    def add_books_metadata(self, books_metadata, calibre_server, library):
        for book_metadata in books_metadata:
            exists = self.__uuid_exists(books_metadata[book_metadata]['uuid'])
            current_server_info = json.loads(calibre_server.server_info)
            if exists:
                print('Book UUID exists in DB, skipping.')
            else:
                book_db_entry = {calibre_server.server_url: books_metadata[book_metadata]}
                book_db_entry[calibre_server.server_url]['library'] = library
                book_db_entry[calibre_server.server_url]['server'] = calibre_server.server_url
                book_db_entry[calibre_server.server_url]['download'] = {}
                try:
                    for book_format in book_db_entry[calibre_server.server_url]['main_format']:
                        book_db_entry[calibre_server.server_url]['download'][book_format] = calibre_server.server_url + book_db_entry[calibre_server.server_url]['main_format'][book_format]
                except:  # TypeError 'NoneType is not iterable
                    pass
                try:
                    for book_format in book_db_entry[calibre_server.server_url]['other_formats']:
                        book_db_entry[calibre_server.server_url]['download'][book_format] = calibre_server.server_url + book_db_entry[calibre_server.server_url]['other_formats'][book_format]
                except:  # TypeError 'NoneType is not iterable
                    pass
                self.cursor.execute("insert into calibre values (?, ?, ?, ?)",
                                    [book_db_entry[calibre_server.server_url]['uuid'], json.dumps(book_db_entry),
                                     calibre_server.server_url, library])
                current_server_info['libraries'][library]['last_indexed'] += 1
                calibre_server.server_info = json.dumps(current_server_info)
        print(calibre_server.server_info)

    def list_libraries(self):
        results = {}
        self.cursor.execute("select * from calibre_servers")
        for result in self.cursor:
            results[result[0]] = json.loads(result[1])
        return results

    def download_library(self, calibre_server, library):
        pass

    def search(self, query, metadata_section):
        def sql_query(q):
            return '%' + q + '%'
        results = []
        if isinstance(query, str):
            self.cursor.execute("select * from calibre where metadata like ?", [sql_query(query)])
            for result in self.cursor:
                dict_result = json.loads(result[1])
                if metadata_section.lower() == 'all':
                    results.append(dict_result)
                    continue
                if isinstance(dict_result[result[2]][metadata_section], list):
                    if any(query.lower() in s.lower() for s in dict_result[result[2]][metadata_section]):
                        results.append(dict_result)
                        continue
                else:
                    if query.lower() in dict_result[result[2]][metadata_section].lower():
                        results.append(dict_result)
                    else:
                        continue
            return results
        if isinstance(query, list):
            if len(query) != 2 or len(metadata_section) != 2:
                print('Provide 2 queries and 2 metadata fields')
                return results
            self.cursor.execute("select * from calibre where metadata like ? and metadata like ?",
                                [sql_query(query[0]), sql_query(query[1])])
            for result in self.cursor:
                dict_result = json.loads(result[1])
                md_section_one = dict_result[result[2]][metadata_section[0]]
                md_section_two = dict_result[result[2]][metadata_section[1]]
                if isinstance(md_section_one, str) and isinstance(md_section_two, str):
                    if query[0].lower() in md_section_one.lower() and query[1].lower() in md_section_two.lower():
                        results.append(dict_result)
                else:
                    if isinstance(md_section_one, list) and isinstance(md_section_two, list):
                        if any(query[0].lower() in s.lower() for s in md_section_one) \
                                and any(query[1].lower() in s.lower() for s in md_section_two):
                            results.append(dict_result)
                    if isinstance(md_section_one, list) and isinstance(md_section_two, str):
                        if any(query[0].lower() in s.lower() for s in md_section_one) \
                                and query[1].lower() in md_section_two.lower():
                            results.append(dict_result)
                    if isinstance(md_section_one, str) and isinstance(md_section_two, list):
                        if query[0].lower() in md_section_one.lower() \
                                and any(query[1].lower() in s.lower() for s in md_section_two):
                            results.append(dict_result)

            return results

    def server_info(self, remote_server):
        query = remote_server
        self.cursor.execute("select * from calibre_servers where server=?", (query,))
        return json.loads(self.cursor.fetchone()[1])

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __server_exists(self, calibre_server):
        self.cursor.execute("select * from calibre_servers where server=?", (calibre_server.server_url,))
        exists = self.cursor.fetchone()
        return True if exists else False

    def __uuid_exists(self, uuid):
        self.cursor.execute("select * from calibre where uuid=?", (uuid,))
        entry = self.cursor.fetchone()
        return True if entry else False

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        if isinstance(exc_val, Exception):
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()


def process_server(calibre_server, db, book_req_limit=500, offset=0):
    saved_offset = offset
    db.add_server_metadata(calibre_server)
    for library in calibre_server.library_info:
        total_num = calibre_server.library_info[library]['total_num']
        offset = saved_offset
        while offset < total_num:
            remaining = min(book_req_limit, total_num - offset)
            book_ids = calibre_server.get_book_ids(library, remaining, offset)
            book_ids_metadata = calibre_server.get_book_ids_metadata(library, book_ids)
            db.add_books_metadata(book_ids_metadata, calibre_server, library)
            db.update_server_metadata(calibre_server)
            db.commit()
            offset += book_req_limit


def search(db, query, metadata_section='all', only_download_links=False):
    results = db.search(query, metadata_section)
    if only_download_links:
        for result in results:
            for server in result:
                for book_format in result[server]['download']:
                    print(result[server]['download'][book_format])
    else:
        for result in results:
            for server in result:
                print('Title: ', result[server]['title'])
                for author in result[server]['authors']:
                    print('Author: ', author)
                print('Languages: ', result[server]['languages'])
                print('Library: ', result[server]['library'])
                print('Download Links: ')
                for book_format in result[server]['download']:
                        print(book_format + ' : ' + result[server]['download'][book_format])
                print('-' * 50)


def list_libraries(db):
    results = db.list_libraries()
    for server in results:
        print('Server: ', server)
        print('Total Books: ', results[server]['total_books'])
        for library in results[server]['libraries']:
            print('  Library: ' + library + ', Books: (' + str(results[server]['libraries'][library]['total_num']) + ')')


def download_server(db, server_url, base_dir):
    def gvf(s):
        s = str(s).strip().replace(' ', '_')
        return re.sub(r'(?u)[^-\w.]', '', s).lower()[:200]

    def get_file(save_file, url):
        r = requests.get(url)
        with open(save_file, 'wb') as f:
            f.write(r.content)

    results = db.search(server_url, 'server')
    server_infos = db.list_libraries()
    errors = []
    count = 1
    total_books = server_infos[server_url]['total_books']
    for result in results:
        for server in result:
            print(str(server) + ' ** downloading ebook: ' + str(count) + ' of ' + str(total_books))
            for book_format in result[server]['download']:
                author = gvf(result[server]['author_sort'].partition(';')[0].rstrip()).lower()
                title = gvf(result[server]['title_sort'])
                save_file = base_dir + '/' + author + '/' + title + '.' + book_format
                url = result[server]['download'][book_format]
                if not os.path.exists(base_dir+'/'+author):
                    os.makedirs(base_dir+'/'+author)
                try:
                    get_file(save_file, url)
                except:
                    errors.append([url, save_file])
                    pass
                # time.sleep(5)
        count += 1
    if errors:
        print('Errors:')
        for error in errors:
            print(error)
    else:
        print('Completed.')




