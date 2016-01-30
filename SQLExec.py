import os
import re
import sublime
import sublime_plugin
import subprocess
import tempfile
from datetime import datetime
from string import Template

connection = None
history = ['']
sqlexec_settings = sublime.load_settings("SQLExec.sublime-settings")


class Connection:
    def __init__(self, options):
        self.query = ''
        self.db_type = options.type
        self.name = '{}: {}@{}'.format(options.type, options.username, options.host)
        self.settings = sublime.load_settings(options.type + ".sqlexec").get('sql_exec')
        self.command = sqlexec_settings.get('sql_exec.commands')[options.type]
        self.options = options

    def _buildCommand(self, options):
        return self.command + ' ' + ' '.join(options) + ' ' + self.settings['args'].format(options=self.options)

    def _getCommand(self, options, queries, header=''):
        if not isinstance(queries, list):
            queries = [queries]
        self.write_query_file(self.settings['before'] + queries)

        command = self._buildCommand(options)
        return Command('%s < "%s"' % (command, self.tmp.name))

    def write_query_file(self, query_list):
        self.tmp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql')
        self.tmp.write("\n".join(query_list))
        self.tmp.close()

    def execute(self, queries):
        command = self._getCommand(self.settings['options'], queries)
        command.show()
        os.unlink(self.tmp.name)

    def desc(self):
        self.query = self.settings['queries']['desc']['query']
        command = self._getCommand(self.settings['queries']['desc']['options'], self.query)

        tables = []
        results = command.run().splitlines()
        if self.db_type == 'sqlite':
            results = ' '.join(results)
            results = results.split()
        for result in results:
            if result and '|' in result:
                try:
                    tables.append(result.split('|')[1].strip())
                except IndexError:
                    pass
            else:
                tables.append(result)
        os.unlink(self.tmp.name)
        return tables

    def descTable(self, tableName):
        self.query = self.settings['queries']['desc table']['query'] % tableName
        command = self._getCommand(self.settings['queries']['desc table']['options'], self.query)
        tabledesc = command.run()

        if self.db_type == 'sqlite':
            tabledesc = SQLite(table_name=tableName, result=tabledesc).get_table_desc()

        command.show(tabledesc)
        os.unlink(self.tmp.name)

    def showTableRecords(self, tableName):
        self.query = self.settings['queries']['show records']['query'] % tableName
        command = self._getCommand(self.settings['queries']['show records']['options'], self.query)
        command.show()

        os.unlink(self.tmp.name)


class Command:

    def __init__(self, text):
        self.text = text
        self.template = Template('$dbinfo    $qtime\nSQL> $query\n\n$result')
        self.show_result_on_window = sqlexec_settings.get('show_result_on_window')

    def _display(self, panel_name, text):
        panel = self.get_panel(panel_name)
        panel.set_read_only(False)
        panel.set_syntax_file(sqlexec_settings.get('syntax'))

        # if len(sqlexec_settings.get('color_scheme')):
        panel.settings().add_on_change('color_scheme', sqlexec_settings.get('color_scheme'))
        panel.settings().set('color_scheme', sqlexec_settings.get('color_scheme'))

        panel.run_command('append', {'characters': self.fill_template(connection.name, connection.query, text)})
        panel.set_read_only(sqlexec_settings.get('read_only_results'))

    def _errors(self, text):
        self._display('SQLExec.errors', text)

    def fill_template(self, connection_name, query, result):
        return self.template.substitute(dbinfo=connection_name, qtime=datetime.now(),
                                        query=re.sub(r'\s+', ' ', query.replace('\n', ' ')),
                                        result=result)

    def get_panel(self, panel_name=None):
        if self.show_result_on_window:
            panel = sublime.active_window().new_file()
            if panel_name:
                panel.set_name(panel_name)
            panel.set_scratch(True)
            return panel
        panel = sublime.active_window().create_output_panel(panel_name)
        sublime.active_window().run_command("show_panel", {"panel": "output." + panel_name})
        return panel

    def run(self):
        sublime.status_message(' SQLExec: running SQL command')
        results, errors = subprocess.Popen(self.text, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()

        if not results and errors:
            self._errors(errors.decode('utf-8', 'replace').replace('\r', ''))

        return results.decode('utf-8', 'replace').replace('\r', '')

    def show(self, results=None):
        if results is None:
            results = self.run()
        if results:
            self._display("SQLExec Results", results)


class Selection:
    def __init__(self, view):
        self.view = view

    def getQueries(self):
        text = []
        if self.view.sel():
            for region in self.view.sel():
                if region.empty():
                    text.append(self.view.substr(self.view.line(region)))
                else:
                    text.append(self.view.substr(region))
        return text


class Options:
    def __init__(self, name):
        self.name = name
        connections = sqlexec_settings.get('connections')
        self.type = connections[self.name]['type']
        self.host = connections[self.name]['host']
        self.port = connections[self.name]['port']
        self.username = connections[self.name]['username']
        self.password = connections[self.name]['password']
        self.database = connections[self.name]['database']
        if 'service' in connections[self.name]:
            self.service = connections[self.name]['service']

    def __str__(self):
        return self.name

    @staticmethod
    def list():
        try:
            return sorted([conn for conn in sqlexec_settings.get('connections')])
        except:
            return []


class SQLite(object):

    result_template = Template('Table: $table\n$data\n\nRaw:\n$raw')

    def __init__(self, table_name=None, result=None, table_meta=None):
        self.table_name = table_name
        self.result = result
        self.table_meta = table_meta

    def _prepare_desc_result(self):
        self.table_name = self.table_name.strip()
        self.raw_data = self.result
        self.table_meta = self.result.split('\n')
        self.table_desc = self.table_meta.pop(0)

    def _prepare_desc_indexes(self):
        self.indexes = list()
        for index in self.table_meta:
            i = self.parse_index(index)
            if i:
                self.indexes.append(i)

    def get_cell_len(self, data, key):
        """ data is a list of dicts and key is a string """
        vals = [str(i[key]) for i in data]
        vals.append(key)
        return len(max([i for i in vals], key=len))

    def parse_results(self, result_list):
        result_list = [i for i in result_list if i != '']

    def get_table_desc(self):
        self._prepare_desc_result()
        self._prepare_desc_indexes()
        self.data = self.format_table_desc()
        return self.result_template.substitute(table=self.table_name, data=self.data, raw=self.raw_data)

    def format_table_desc(self):
        self.table_desc = self.table_desc.strip()
        self.table_desc = re.sub(r'CREATE TABLE "{}" \('.format(self.table_name), '', self.table_desc)
        self.table_desc = re.sub(r'\)\;$', '', self.table_desc)
        self.table_desc = self.table_desc.replace('"', '')

        rows = list()
        uniq = list()
        m = re.match(r".*UNIQUE\s\((.*)\)", self.table_desc)
        if m:
            try:
                uniq = m.group(1)
                uniq = uniq.split(',')
                self.table_desc = re.sub(r"\, UNIQUE\s\(.*\)", '', self.table_desc)
            except:
                uniq = list()

        parselist = self.table_desc.split(',')
        parselist = [p.strip() for p in parselist]

        for row in parselist:
            fields = dict()
            row = row.split(' ', 2)

            fields['NAME'] = row.pop(0)
            fields['TYPE'] = row.pop(0)
            fields['INDEX'] = '' if fields['NAME'] not in self.indexes else 'X'
            fields['UNIQUE'] = '' if fields['NAME'] not in uniq else 'X'
            fields['ATTRIBUTES'] = ' '.join(row)
            rows.append(fields)

        name_len = self.get_cell_len(rows, 'NAME')
        type_len = self.get_cell_len(rows, 'TYPE')
        index_len = self.get_cell_len(rows, 'INDEX')
        unique_len = self.get_cell_len(rows, 'UNIQUE')
        attr_len = self.get_cell_len(rows, 'ATTRIBUTES')

        header_formatter = '| {:^%s} | {:^%s} | {:^%s} | {:^%s} | {:^%s} |' % (name_len, type_len, index_len, unique_len, attr_len)
        data_formatter = '| {:<%s} | {:<%s} | {:^%s} | {:^%s} | {:<%s} |' % (name_len, type_len, index_len, unique_len, attr_len)
        the_table_tr_border = '+-{}-+-{}-+-{}-+-{}-+-{}-+'.format('-'*name_len, '-'*type_len, '-'*index_len, '-'*unique_len, '-'*attr_len)

        the_table = '{}\n{}\n{}\n'.format(the_table_tr_border,
                                          header_formatter.format('NAME', 'TYPE', 'INDEX', 'UNIQUE', 'ATTRIBUTES'),
                                          the_table_tr_border)
        for row in rows:
            the_table = '{}{}\n'.format(the_table,
                                        data_formatter.format(row['NAME'], row['TYPE'], row['INDEX'], row['UNIQUE'], row['ATTRIBUTES']))
        return '{}{}'.format(the_table, the_table_tr_border)

    def parse_index(self, table_meta):
        m = re.match(r"^CREATE\sINDEX \"\w+\"\sON\s\"\w+\"\s\(\"(\w+)\"\);", table_meta)
        try:
            return m.group(1)
        except:
            pass
        return None


def sqlChangeConnection(index):
    global connection
    names = Options.list()
    options = Options(names[index])
    connection = Connection(options)
    sublime.status_message(' SQLExec: switched to %s' % names[index])


def showTableRecords(index):
    global connection
    if index > -1:
        if connection is None:
            return sublime.error_message('No active connection')
        tables = connection.desc()
        connection.showTableRecords(tables[index])


def descTable(index):
    global connection
    if index > -1:
        if connection is None:
            return sublime.error_message('No active connection')
        tables = connection.desc()
        connection.descTable(tables[index])


def executeHistoryQuery(index):
    global history
    if index > -1:
        executeQuery(history[index])


def executeQuery(query):
    global connection
    global history
    history.append(query)
    history = list(set(history))
    if connection is not None:
        connection.execute(query)


class sqlHistory(sublime_plugin.WindowCommand):
    global history
    def run(self):
        sublime.active_window().show_quick_panel(history, executeHistoryQuery)


class sqlDesc(sublime_plugin.WindowCommand):
    def run(self):
        global connection
        if connection is None:
            return sublime.error_message('No active connection')
        tables = connection.desc()
        sublime.active_window().show_quick_panel(tables, descTable)


class sqlShowRecords(sublime_plugin.WindowCommand):
    def run(self):
        global connection
        if connection is None:
            return sublime.error_message('No active connection')
        tables = connection.desc()
        sublime.active_window().show_quick_panel(tables, showTableRecords)


class sqlQuery(sublime_plugin.WindowCommand):
    def run(self):
        global connection
        global history
        if connection is None:
            return sublime.error_message('No active connection')
        sublime.active_window().show_input_panel('Enter query', history[-1], executeQuery, None, None)


class sqlExecute(sublime_plugin.WindowCommand):
    def run(self):
        global connection
        if connection is None:
            return sublime.error_message('No active connection')
        selection = Selection(self.window.active_view())
        connection.execute(selection.getQueries())


class sqlListConnection(sublime_plugin.WindowCommand):
    def run(self):
        sublime.active_window().show_quick_panel(Options.list(), sqlChangeConnection)
