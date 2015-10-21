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
        for result in command.run().splitlines():
            try:
                tables.append(result.split('|')[1].strip())
            except IndexError:
                pass

        os.unlink(self.tmp.name)
        return tables

    def descTable(self, tableName):
        self.query = self.settings['queries']['desc table']['query'] % tableName
        command = self._getCommand(self.settings['queries']['desc table']['options'], self.query)
        command.show()

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

    def show(self):
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
        self.name     = name
        connections   = sqlexec_settings.get('connections')
        self.type     = connections[self.name]['type']
        self.host     = connections[self.name]['host']
        self.port     = connections[self.name]['port']
        self.username = connections[self.name]['username']
        self.password = connections[self.name]['password']
        self.database = connections[self.name]['database']
        if 'service' in connections[self.name]:
            self.service  = connections[self.name]['service']

    def __str__(self):
        return self.name

    @staticmethod
    def list():
        names = []
        connections = sqlexec_settings.get('connections')
        for connection in connections:
            names.append(connection)
        names.sort()
        return names


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
            sublime.error_message('No active connection')
            return
        tables = connection.desc()
        connection.showTableRecords(tables[index])


def descTable(index):
    global connection
    if index > -1:
        if connection is None:
            sublime.error_message('No active connection')
            return
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
            sublime.error_message('No active connection')
            return
        tables = connection.desc()
        sublime.active_window().show_quick_panel(tables, descTable)


class sqlShowRecords(sublime_plugin.WindowCommand):
    def run(self):
        global connection
        if connection is None:
            sublime.error_message('No active connection')
            return
        tables = connection.desc()
        sublime.active_window().show_quick_panel(tables, showTableRecords)


class sqlQuery(sublime_plugin.WindowCommand):
    def run(self):
        global connection
        global history
        if connection is None:
            sublime.error_message('No active connection')
            return
        sublime.active_window().show_input_panel('Enter query', history[-1], executeQuery, None, None)


class sqlExecute(sublime_plugin.WindowCommand):
    def run(self):
        global connection
        if connection is None:
            sublime.error_message('No active connection')
            return
        selection = Selection(self.window.active_view())
        connection.execute(selection.getQueries())


class sqlListConnection(sublime_plugin.WindowCommand):
    def run(self):
        sublime.active_window().show_quick_panel(Options.list(), sqlChangeConnection)
