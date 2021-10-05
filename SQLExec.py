import sublime, sublime_plugin, os, re, subprocess
import tempfile
from datetime import datetime
from string import Template

connection = None
debug = sqlexec_settings.get('sql_exec.debug')
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
        return (self.command + ' ' + ' '.join(options) + ' ' +
            self.settings['args'].format(options=self.options))

    def _getCommand(self, options, queries, header=''):
        if not isinstance(queries, list):
            queries = [queries]
        self.write_query_file(self.settings['before'] + queries)

        command = self._buildCommand(options)
        return Command('%s < "%s"' % (command, self.tmp.name), self.options.encoding)

    def write_query_file(self, query_list):
        self.tmp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql')
        self.tmp.write("\n".join(query_list))
        self.tmp.close()

    def setDatabaes(self, index):
        self.options.database = self.tempArray[index]

    def execute(self, queries, export = False):
        command = self._getCommand(self.settings['options'], queries)
        command.show(export)
        os.unlink(self.tmp.name)

    def showDatabases(self):
        query = self.settings['queries']['show databases']['query']
        command = self._getCommand(self.settings['queries']['show databases']['options'], query)

        db = []
        command.show()
        for result in command.run().splitlines():
            try:
                db.append(result.split('|')[1].strip())
            except IndexError:
                pass
        os.unlink(self.tmp.name)

        self.tempArray = db
        return db

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

    def listFunc(self):
        query = '\df'
        command = self._getCommand([], query)

        funcs = []
        for result in command.run().splitlines():
            try:
                ln = result.split('|')
                funcs.append('{}.{}'.format(ln[0].strip(), ln[1].strip()))
            except IndexError:
                pass

        os.unlink(self.tmp.name)

        return funcs

    def descTable(self, tableName):
        self.query = self.settings['queries']['desc table']['query'] % tableName
        command = self._getCommand(self.settings['queries']['desc table']['options'], self.query)
        tabledesc = command.run()

        if self.db_type == 'sqlite':
            tabledesc = SQLite(table_name=tableName, result=tabledesc).get_table_desc()
        
        command.show(False,tabledesc)
        os.unlink(self.tmp.name)

    def descFunc(self, tableName):
        command = self._getCommand([], '\sf {}'.format(tableName))
        command.show()
        os.unlink(self.tmp.name)

    def showTableRecords(self, tableName):
        self.query = self.settings['queries']['show records']['query'] % tableName
        command = self._getCommand(self.settings['queries']['show records']['options'], self.query)
        command.show()

        os.unlink(self.tmp.name)

class Command:
    def __init__(self, text, encoding = None):
        self.text = text
        self.encoding = encoding
        self.template = Template('$dbinfo    $qtime\nSQL> $query\n\n$result')
        self.show_result_on_window = sqlexec_settings.get('show_result_on_window')

    def _clean_text(self, text):
        lines = text.split('\n')
        csv_sep = sqlexec_settings.get('csv_separator', ';')
        tabularData = [];
        for line in lines:
            mat = re.search('((-+)|)(\+)((-+)|)',line)
            if mat:
                continue
            if line[0:1] == "|":
                line = line[1:]
            if line.endswith("|"):
                line = line[:-1]
            columns = re.split('(?<!\\\B)\|',line);
            tableCol = []
            for col in columns:
                col = col.strip()
                #if col.find(csv_sep) > 0:
                col = '"' + col + '"';
                col = re.sub('/\\\|/',"|",col);
                tableCol.append(col)
            tabularData.append(csv_sep.join(tableCol))
        return "\n".join(tabularData)

    def _display(self, panelName, text, export = False):
        panel = self.get_panel(panelName,export)
        panel.set_read_only(False)
        panel.set_syntax_file(sqlexec_settings.get('syntax'))

        text = self._clean_text(text) if export else text

        panel.settings().add_on_change('color_scheme', sqlexec_settings.get('color_scheme'))
        panel.settings().set('color_scheme', sqlexec_settings.get('color_scheme'))

        if export:
            panel.run_command('append', {'characters': text})
            panel.set_name(panelName+".csv")
            panel.set_read_only(True)
            panel.run_command('save')
        else:
            panel.run_command('append', {'characters': self.fill_template(connection.name, connection.query, text)})
            panel.set_read_only(sqlexec_settings.get('read_only_results'))


    def _display_tab(self, text):
        view = sublime.active_window().new_file()
        view.run_command('show_text', {'txt': text})
        view.set_name('SQLExec')
        view.set_syntax_file('Packages/SQL/SQL.tmLanguage')
        view.set_scratch(True)

    def _result(self, text, export = False):
        self._display('SQLExec Results', text, export)

    def _errors(self, text):
        self._display('SQLExec.errors', text)

    def fill_template(self, connection_name, query, result):
        return self.template.substitute(dbinfo=connection_name, qtime=datetime.now(),
                                        query=re.sub(r'\s+', ' ', query.replace('\n', ' ')),
                                        result=result)

    def get_panel(self, panel_name=None, export = False):
        if self.show_result_on_window or export:
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
        if debug:
            sublime.message_dialog('Command: ' + str(self.text))
        results, errors = subprocess.Popen(self.text, stdout=subprocess.PIPE,stderr=subprocess.PIPE, shell=True, env=os.environ.copy()).communicate()

        encoding = 'utf-8' if self.encoding is None else self.encoding
        if not results and errors:
            self._errors(errors.decode(encoding, 'replace').replace('\r', ''))

        return results.decode(encoding, 'replace').replace('\r', '')

    def show(self, export = False, results=None):
        if results is None:
           results = self.run()
        else:
            self._display("SQLExec Results", results)

        if results:
            self._result(results, export)

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
        if 'password' in connections[self.name]:
            self.password = connections[self.name]['password']
        self.database = connections[self.name]['database']
        self.encoding = connections[self.name].get('encoding')
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
    try:
        options = Options(names[index])
        connection = Connection(options)
        sublime.status_message(' SQLExec: switched to %s' % names[index])
        sublime.active_window().active_view().run_command('sql_show_active_connection')
    except IndexError:
        sublime.status_message(' SQLExec Error: %s is not configured in SQLExec settings' % index)


def showTableRecords(index):
    if index > -1:
        if connection is None:
            return sublime.error_message('No active connection')
        tables = connection.desc()
        connection.showTableRecords(tables[index])



def descTable(index):
    if index > -1:
        if connection is None:
            return sublime.error_message('No active connection')
        tables = connection.desc()
        connection.descTable(tables[index])



def descFunc(index):
    if index > -1:
        if connection is not None:
            funcs = connection.listFunc()
            connection.descFunc(funcs[index])
        else:
            sublime.error_message('No active connection')


def executeHistoryQuery(index):
    if index > -1:
        executeQuery(history[index])


def executeQuery(query):
    global history
    if query not in history:
        history.append(query)
    if connection is not None:
        connection.execute(query)


class sqlHistory(sublime_plugin.WindowCommand):

    def run(self):
        sublime.active_window().show_quick_panel(history, executeHistoryQuery)


class sqlDesc(sublime_plugin.WindowCommand):
    def run(self):
        if connection is not None:
            tables = connection.desc()
            sublime.active_window().show_quick_panel(tables, descTable)
        else:
            sublime.error_message('No active connection')

class sqlShowDatabases(sublime_plugin.WindowCommand):
    def run(self):
        if connection != None:
            db = connection.showDatabases()
            # //sublime.error_message('No active ssssssss')
            sublime.active_window().show_quick_panel(db, setDatabaes)
        else:
            sublime.error_message('No active connection')

class sqlShowFunction(sublime_plugin.WindowCommand):
    def run(self):
        if connection is not None:
            funcs = connection.listFunc()
            sublime.active_window().show_quick_panel(funcs, descFunc)
        else:
            sublime.error_message('No active connection')


class sqlShowRecords(sublime_plugin.WindowCommand):
    def run(self):
        if connection is not None:
            tables = connection.desc()
            sublime.active_window().show_quick_panel(tables, showTableRecords)
        else:
            sublime.error_message('No active connection')


class sqlQuery(sublime_plugin.WindowCommand):
    def run(self):
        if connection is not None:
            sublime.active_window().show_input_panel('Enter query', history[-1],
                executeQuery, None, None)
        else:
            sublime.error_message('No active connection')


class sqlExecute(sublime_plugin.WindowCommand):
    def run(self):
        if connection is not None:
            selection = Selection(self.window.active_view())
            connection.execute(selection.getQueries())
        else:
            sublime.error_message('No active connection')

class sqlExport(sublime_plugin.WindowCommand):
    def run(self):
        global connection
        if connection != None:
            selection = Selection(self.window.active_view())
            connection.execute(selection.getQueries(), export = True)
        else:
            sublime.error_message('No active connection')

class sqlListConnection(sublime_plugin.WindowCommand):
    def run(self):
        sublime.active_window().show_quick_panel(Options.list(), sqlChangeConnection)

class sqlExecListener(sublime_plugin.EventListener):
    def on_activated(self, view):
        view.run_command('sql_show_active_connection')

class sqlShowActiveConnection(sublime_plugin.TextCommand):
    def run(self, view):
        self.status_bar()

    def status_bar(self):
        global connection
        icon = sqlexec_settings.get('connection_icon') or '\u26A1'

        message =  'SQLExec conn %s : %s ' % (icon, connection.options)
        sublime.active_window().active_view().set_status('sqlexec', message)
