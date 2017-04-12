import sublime, sublime_plugin, tempfile, os, subprocess
from time import sleep
import threading

connection = None
history = ['']


class Connection:
    def __init__(self, options):
        self.settings = sublime.load_settings(options.type + ".sqlexec").get(
            'sql_exec')
        self.command  = sublime.load_settings("SQLExec.sublime-settings").get(
            'sql_exec.commands')[options.type]
        self.options  = options

    def _buildCommand(self, options):
        return (self.command + ' ' + ' '.join(options) + ' ' +
            self.settings['args'].format(options=self.options))

    def _getCommand(self, options, queries, header=''):
        command  = self._buildCommand(options)
        self.tmp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql')
        for query in self.settings['before']:
            self.tmp.write(query + "\n")
        for query in queries:
            self.tmp.write(query)
        self.tmp.close()

        cmd = '%s < "%s"' % (command, self.tmp.name)

        return Command(cmd)

    def execute(self, queries):
        command = self._getCommand(self.settings['options'], queries)
        command.show_eventually(self.tmp.name)
        # os.unlink(self.tmp.name)

    def desc(self):
        query = self.settings['queries']['desc']['query']
        command = self._getCommand(self.settings['queries']['desc']['options'],
            query)

        tables = []
        for result in command.run().splitlines():
            try:
                tables.append(result.split('|')[1].strip())
            except IndexError:
                pass

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
        query = self.settings['queries']['desc table']['query'] % tableName
        command = self._getCommand(self.settings['queries']['desc table']['options'],
            query)
        command.show()

        os.unlink(self.tmp.name)

    def descFunc(self, tableName):
        command = self._getCommand([], '\sf {}'.format(tableName))
        command.show()

        os.unlink(self.tmp.name)

    def showTableRecords(self, tableName):
        query = self.settings['queries']['show records']['query'] % tableName
        command = self._getCommand(self.settings['queries']['show records'][
            'options'], query)
        command.show()

        os.unlink(self.tmp.name)


class Command:
    def __init__(self, text):
        self.text = text

    def _display(self, panelName, text):
        if not sublime.load_settings("SQLExec.sublime-settings").get(
                'show_result_on_window'):
            panel = sublime.active_window().create_output_panel(panelName)
            sublime.active_window().run_command("show_panel",
                {"panel": "output." + panelName})
        else:
            panel = sublime.active_window().new_file()

        panel.set_read_only(False)
        panel.set_syntax_file('Packages/SQL/SQL.tmLanguage')
        panel.run_command('append', {'characters': text})
        panel.set_read_only(True)

    def _display_tab(self, text):
        view = sublime.active_window().new_file()
        view.run_command('show_text', {'txt': text})
        view.set_name('SQLExec')
        view.set_syntax_file('Packages/SQL/SQL.tmLanguage')
        view.set_scratch(True)

    def _result(self, text):
        self._display('SQLExec', text)

    def _errors(self, text):
        self._display('SQLExec.errors', text)

    def run(self):
        sublime.status_message(' SQLExec: running SQL command')
        print(self.text)

        results, errors = subprocess.Popen(self.text, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, shell=True).communicate()

        if not results and errors:
            self._errors(errors.decode('utf-8', 'replace').replace('\r', ''))

        return results.decode('utf-8', 'replace').replace('\r', '')

    def show(self):
        results = self.run()

        if results:
            self._result(results)

    def show_eventually(self, tmp_name):
        text = self.text
        print(text)
        def _run():
            tmp_out = tempfile.TemporaryFile(suffix='.txt')
            tmp_err = tempfile.TemporaryFile(suffix='.txt')
            proc = subprocess.Popen(text, stdout=tmp_out,
                stderr=tmp_err, shell=True)

            sublime.status_message(' SQLExec: processing query')
            i = 0
            while True:
                if proc.poll() is not None: break
                sleep(1)
                i += 1
            sublime.status_message(' SQLExec: finished in {} seconds'.format(i))

            # with open(tmp_out.name, 'rb') as out, open(tmp_err.name, 'rb') as err:
                # results = out.read().decode('utf-8', 'replace')
                # errors  = err.read().decode('utf-8', 'replace')
            tmp_out.seek(0)
            tmp_err.seek(0)
            results = tmp_out.read().decode('utf-8', 'replace')
            errors  = tmp_err.read().decode('utf-8', 'replace')
            os.unlink(tmp_name)
            # os.unlink(tmp_out.name)
            # os.unlink(tmp_err.name)

            if not results and errors:
                self._errors(errors.replace('\r', ''))
            else:
                res = results.replace('\r', '') + 'roughly {} seconds\n'.format(i)
                if res.count('\n') < 40: self._result(res)
                else:                    self._display_tab(res)


        threading.Thread(target=_run).start()

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
        connections   = sublime.load_settings("SQLExec.sublime-settings").get(
            'connections')
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
        connections = sublime.load_settings("SQLExec.sublime-settings").get(
            'connections')
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
    if index > -1:
        if connection is not None:
            tables = connection.desc()
            connection.showTableRecords(tables[index])
        else:
            sublime.error_message('No active connection')


def descTable(index):
    if index > -1:
        if connection is not None:
            tables = connection.desc()
            connection.descTable(tables[index])
        else:
            sublime.error_message('No active connection')


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


class sqlListConnection(sublime_plugin.WindowCommand):
    def run(self):
        sublime.active_window().show_quick_panel(Options.list(), sqlChangeConnection)
