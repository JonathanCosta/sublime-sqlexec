import sublime, sublime_plugin, os, re, subprocess, signal, sys
from time import time, sleep
from threading import Thread, Timer

connection = None
history = []

def filter_dupes(items):
    uniqs = []
    for i in items:
        if i not in uniqs:
            uniqs.append(i)
    return uniqs

def getSelectionQueries(view):
    return "".join([view.substr(view.line(region) if region.empty() else region) for region in view.sel()])

def input_panel(caption, initial='', on_done=None, on_change=None, on_cancel=None):
    return sublime.active_window().show_input_panel(caption, initial or '', on_done, on_change, on_cancel)

def quick_select(items, on_select, selected_index=None):
    items = filter_dupes(items)

    if not hasattr(quick_select, 'cached_items'):
        setattr(quick_select, 'cached_items', {})
    cached_items = getattr(quick_select, 'cached_items', {})

    def select(index):
        if 0 <= index < len(items):
            cached_items[hash(tuple(items))] = index
            setattr(quick_select, 'cached_items', cached_items)
            on_select(items[index])

    if selected_index is None or selected_index < 0:
        selected_index = cached_items.get(hash(tuple(items)), 0)

    sublime.active_window().show_quick_panel(items, select, selected_index=selected_index)

def status_message(message):
    sublime.status_message(" SQLExec: {}".format(message))

def settings(name="SQLExec.sublime-settings"):
    return sublime.load_settings(name)

class Nop(object):
    def nop(*args, **kw):
        pass

    def __getattr__(self, _):
        return self.nop

def con():
    global connection
    if connection is not None:
        return connection
    else:
        sublime.error_message('No active connection')
        return Nop()

class Connection:
    def __init__(self, name):
        options = Options(name)
        self.settings = settings(options.type + ".sqlexec").get('sql_exec')
        self.command  = [
            settings().get('commands')[options.type],
            self.settings['args'].format(**options)
        ]
        self.options  = options
        self.active_command = None

    def _parseResults(self, results):
        results = [[v.strip() for v in row.split('|')[1:-1]] if '|' in row else [row.strip()] for row in results.strip().splitlines()]
        return [r[0] for r in results] if results and len(results[0]) == 1 else results

    def _display(self, results, elapsed):
        if not results:
            return

        if not settings().get('show_result_on_window'):
            panel = sublime.active_window().create_output_panel("SQLExec")
            sublime.active_window().run_command("show_panel", {"panel": "output." + "SQLExec"})
        else:
            panel = sublime.active_window().new_file()

        panel.settings().set("word_wrap", "false")
        panel.set_read_only(False)

        # Huge amounts of text tend to freeze up Sublime's UI
        if len(results) > 10000000:
            panel.set_syntax_file('Packages/Text/Plain text.tmLanguage')
            for i in range(0, len(results), 1000000):
                panel.run_command('append', {'characters': results[i:i+1000000]})
        elif results:
            panel.set_syntax_file('Packages/SQL/SQL.sublime-syntax')
            panel.run_command('append', {'characters': results})

        status_message('Query executed in {:.3f}s'.format(elapsed))
        panel.set_read_only(True)

    def _execute(self, args, query, cb, run_async=True):
        def cleanup(*args):
            self.active_command = None
            cb(*args)
        if self.active_command:
            self.active_command.stop()

        if run_async:
            self.active_command = Command.start_async(args, query, cleanup)
        else:
            self.active_command = Command(args, query, cleanup)
            self.active_command.run()

    def execute(self, query):
        args = self.command + self.settings['options']
        self._execute(args, query, self._display)

    def explain(self, query):
        args = self.command + self.settings['queries']['explain']['options']
        query = self.settings['queries']['explain']['query'] % query
        self._execute(args, query, self._display)

    def getTables(self, cb, run_async=True):
        args = self.command + self.settings['queries']['desc']['options']
        query = self.settings['queries']['desc']['query']
        self._execute(args, query, (lambda results, elapsed: cb(self._parseResults(results))), run_async)

    def getFunctions(self, cb, run_async=True):
        args = self.command + self.settings['queries']['func list']['options']
        query = self.settings['queries']['func list']['query']
        self._execute(args, query, (lambda results, elapsed: cb(self._parseResults(results))), run_async)

    def getColumns(self, cb, run_async=True):
        args = self.command + self.settings['queries']['column list']['options']
        query = self.settings['queries']['column list']['query']
        self._execute(args, query, (lambda results, elapsed: cb(self._parseResults(results))), run_async)

    def showRecentTableRecords(self, tableName):
        args = self.command + self.settings['queries']['show recent records']['options']
        query = self.settings['queries']['show recent records']['query'] % (tableName, tableName.split('.')[-1])
        self._execute(args, query, self._display)

    def showTableRecords(self, tableName):
        args = self.command + self.settings['queries']['show records']['options']
        query = self.settings['queries']['show records']['query'] % (tableName, tableName.split('.')[-1])
        self._execute(args, query, self._display)

    def descTable(self, tableName):
        args = self.command + self.settings['queries']['desc table']['options']
        query = self.settings['queries']['desc table']['query'] % tableName
        self._execute(args, query, self._display)

    def descFunc(self, funcName):
        args = self.command + self.settings['queries']['desc func']['options']
        query = self.settings['queries']['desc func']['query'] % funcName
        self._execute(args, query, self._display)

    def descColumn(self, columnName):
        args = self.command + self.settings['queries']['desc column']['options']
        query = self.settings['queries']['desc column']['query'] % columnName
        self._execute(args, query, self._display)

class StatusSpinner(Thread):
    def __init__(self, thread):
        super().__init__(self)
        self.watched_thread = thread

    def run(self):
        start_time = time()
        while self.watched_thread.isAlive():
            status_message("{:.0f}s".format(time() - start_time))
            sleep(0.5)

class Command(Thread):
    def __init__(self, args, query, on_done):
        super().__init__(self)
        self.query = query
        self.on_done = on_done
        self.command_text = " ".join(args)

    def run(self):
        re_endings = re.compile(r'\\r|\s*\+\s*$')
        decode = lambda t: re_endings.sub('', t.decode('utf-8', 'replace')).rstrip()

        start_time = time()

        startupinfo = None
        if os.name == "nt":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

        self.process = subprocess.Popen(self.command_text, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.PIPE, startupinfo=startupinfo)
        self.process.stdin.write(self.query.encode())
        self.process.stdin.close()

        results = "\n".join([decode(l) for l in self.process.stdout])
        self.on_done(results, time() - start_time)

    def stop(self):
        if not self.process or self.process.poll() is not None:
            return

        try:
            if sys.platform == 'win32':
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags != subprocess.STARTF_USESHOWWINDOW
                subprocess.Popen("taskkill /F /PID " + str(self.process.pid), startupinfo=startupinfo)
            else:
                os.kill(self.process.pid, getattr(signal, 'SIGKILL', signal.SIGTERM))
            self.process = None
            return True
        except Exception:
            pass

    @staticmethod
    def start_async(args, query, on_done):
        command = Command(args, query, on_done)
        command.daemon = True
        command.start()

        spinner = StatusSpinner(command)
        spinner.daemon = True
        spinner.start()

        try:
            timeout = float(settings().get("query_timeout"))
            if timeout > 0:
                def timed_out():
                    if command.stop():
                        status_message('Query exceeded {}s timeout and was killed'.format(timeout))
                kill_timer = Timer(timeout, timed_out)
                kill_timer.start()
        except Exception:
            pass

        return command

class Options(dict):
    def __init__(self, name):
        super().__init__(self)
        self['name'] = name
        self.update(settings().get('connections').get(name, {}))

    def __getattr__(self, key):
        return self.get(key, None)

    @staticmethod
    def list():
        return sorted(settings().get("connections"))

def sqlChangeConnection(name):
    global connection
    connection = Connection(name)
    status_message('Switched to ' + name)

def executeQuery(query):
    global history
    history = filter_dupes([query] + history)[:50]
    con().execute(query)

def explainQuery(query):
    global history
    history = filter_dupes([query] + history)[:50]
    con().explain(query)

class sqlHistory(sublime_plugin.WindowCommand):
    def run(self):
        if history:
            quick_select(history, executeQuery)
        else:
            status_message('History is Empty! Go run a query!')

class sqlEditHistory(sublime_plugin.WindowCommand):
    def run(self):
        quick_select(history, lambda query: input_panel('Enter query', query, executeQuery))

class sqlDesc(sublime_plugin.WindowCommand):
    def run(self):
        con().getTables(lambda tables: quick_select(tables, con().descTable), False)

class sqlDescFunc(sublime_plugin.WindowCommand):
    def run(self):
        con().getFunctions(lambda functions: quick_select(functions, con().descFunc), False)

class sqlShowRecentRecords(sublime_plugin.WindowCommand):
    def run(self):
        con().getTables(lambda tables: quick_select(tables, con().showRecentTableRecords), False)

class sqlShowRecords(sublime_plugin.WindowCommand):
    def run(self):
        con().getTables(lambda tables: quick_select(tables, con().showTableRecords), False)

class sqlQuery(sublime_plugin.WindowCommand):
    def run(self):
        input_panel('Enter query', history[0] if history else None, executeQuery)

class sqlExplainQuery(sublime_plugin.WindowCommand):
    def run(self):
        input_panel('Enter query', history[0] if history else None, explainQuery)

class sqlColumn(sublime_plugin.WindowCommand):
    def run(self):
        con().getColumns(lambda columns: quick_select(columns, con().descColumn), False)

class sqlExecute(sublime_plugin.WindowCommand):
    def run(self):
        con().execute(getSelectionQueries(self.window.active_view()))

class sqlExplain(sublime_plugin.WindowCommand):
    def run(self):
        con().explain(getSelectionQueries(self.window.active_view()))

class sqlListConnection(sublime_plugin.WindowCommand):
    def run(self):
        quick_select(Options.list(), sqlChangeConnection, next((i for i, name in enumerate(Options.list()) if Options(name).is_default), 0))

def defaultConnection():
    name = next((name for name in Options.list() if Options(name).is_default), None)
    if name:
        sqlChangeConnection(name)

sublime.set_timeout_async(defaultConnection, 500)
