from thrift import TTornado
from thrift.protocol import TCompactProtocol
from tornado import gen
from pyaccumulo import BaseIterator, Cell, _get_scan_columns
from pyaccumulo.tornado.proxy import AccumuloProxy
from pyaccumulo.tornado.proxy.ttypes import TimeType, WriterOptions, IteratorSetting, ScanOptions, BatchScanOptions


BW_DEFAULTS = dict(
    max_memory=10*1024,
    latency_ms=30*1000,
    timeout_ms=5*1000,
    threads=10,
)

# The number of entries returned with a single scan.next()
SCAN_BATCH_SIZE = 10


class BatchWriter(object):
    def __init__(self, conn):
        super(BatchWriter, self).__init__()
        self.client = conn.client
        self.login = conn.login
        self._writer = None

    @staticmethod
    @gen.engine
    def create(conn, table, max_memory, latency_ms, timeout_ms, threads, callback):
        bw = BatchWriter(conn)
        bw_options = WriterOptions(maxMemory=max_memory, latencyMs=latency_ms, timeoutMs=timeout_ms, threads=threads)
        bw._writer = yield gen.Task(bw.client.createWriter, bw.login, table, bw_options)
        bw._is_closed = False
        callback(bw)

    @gen.engine
    def add_mutations(self, muts, callback):
        if self._writer is None:
            raise Exception("Cannot write to a closed writer")
        cells = {}
        for mut in muts:
            cells.setdefault(mut.row, []).extend(mut.updates)
        yield gen.Task(self.client.update, self._writer, cells)
        callback()

    @gen.engine
    def add_mutation(self, mut, callback):
        if self._writer is None:
            raise Exception("Cannot write to a closed writer")
        yield gen.Task(self.client.update, self._writer, {mut.row: mut.updates})
        callback()

    @gen.engine
    def flush(self, callback):
        if self._writer is None:
            raise Exception("Cannot flush a closed writer")
        yield gen.Task(self.client.flush, self._writer)
        callback()

    @gen.engine
    def close(self, callback):
        yield gen.Task(self.client.closeWriter, self._writer)
        self._writer = None
        callback()


class Scanner(object):
    def __init__(self, conn, scanrange=None, cols=None, auths=None, iterators=None, bufsize=None,
                 batchsize=SCAN_BATCH_SIZE):
        super(Scanner, self).__init__()
        self.client = conn.client
        self.login = conn.login
        self._scanner = None
        self.options = ScanOptions(auths, self._get_range(scanrange), _get_scan_columns(cols),
                                   self._get_iterator_settings(iterators), bufsize)
        self.batchsize = batchsize
        self.batch = None

    @staticmethod
    def _get_range(scanrange):
        if scanrange:
            return scanrange.to_range()
        else:
            return None

    @staticmethod
    def _get_ranges(scanranges):
        if scanranges:
            return [scanrange.to_range() for scanrange in scanranges]
        else:
            return None

    def _get_iterator_settings(self, iterators):
        if not iterators:
            return None
        return [self._process_iterator(i) for i in iterators]

    @staticmethod
    def _process_iterator(iterator):
        if isinstance(iterator, IteratorSetting):
            return iterator
        elif isinstance(iterator, BaseIterator):
            return iterator.get_iterator_setting()
        else:
            raise Exception("Cannot process iterator: %s" % iterator)

    @staticmethod
    @gen.engine
    def create(conn, table, scanrange, cols, auths, iterators, callback):
        scanner = Scanner(conn, scanrange, cols, auths, iterators)
        scanner._scanner = yield gen.Task(scanner.client.createScanner, scanner.login, table, scanner.options)
        callback(scanner)

    @gen.engine
    def next(self, callback):
        self.batch = yield gen.Task(self.client.nextK, self._scanner, self.batchsize)
        entries = []
        if self.batch.more:
            entries = [Cell(e.key.row, e.key.colFamily, e.key.colQualifier, e.key.colVisibility, e.key.timestamp,
                            e.value) for e in self.batch.results]
        callback(entries)

    def has_next(self):
        if self.batch is None:
            return True
        return self.batch.more

    @gen.engine
    def close(self, callback):
        yield gen.Task(self.client.closeScanner, self._scanner)
        callback()


class Accumulo(object):
    def __init__(self, host="localhost", port=50096):
        super(Accumulo, self).__init__()
        self.transport = TTornado.TTornadoStreamTransport(host, port)
        self.pfactory = TCompactProtocol.TCompactProtocolFactory()
        self.client = AccumuloProxy.Client(self.transport, self.pfactory)
        self.login = None

    @staticmethod
    @gen.engine
    def create_and_connect(host, port, user, password, callback):
        acc = Accumulo(host, port)
        yield gen.Task(acc.connect, user, password)
        callback(acc)

    @gen.engine
    def connect(self, user, password, callback):
        yield gen.Task(self.transport.open)
        self.login = yield gen.Task(self.client.login, user, {'password': password})
        callback()

    def close(self):
        self.transport.close()

    @gen.engine
    def list_tables(self, callback):
        tables = yield gen.Task(self.client.listTables, self.login)
        tables = [t for t in tables]
        callback(tables)

    @gen.engine
    def table_exists(self, table, callback):
        res = gen.Task(self.client.tableExists, self.login, table)
        callback(res)

    @gen.engine
    def create_table(self, table, callback):
        yield gen.Task(self.client.createTable, self.login, table, True, TimeType.MILLIS)
        callback()

    @gen.engine
    def delete_table(self, table, callback):
        yield gen.Task(self.client.deleteTable, self.login, table)
        callback()

    @gen.engine
    def rename_table(self, oldtable, newtable, callback):
        yield gen.Task(self.client.renameTable, self.login, oldtable, newtable)
        callback()

    @gen.engine
    def write(self, table, muts, callback):
        if not isinstance(muts, list) and not isinstance(muts, tuple):
            muts = [muts]

        writer = yield gen.Task(self.create_batch_writer, table)
        yield gen.Task(writer.add_mutations, muts)
        yield gen.Task(writer.close)
        callback()

    @gen.engine
    def create_scanner(self, table, scanrange, cols, auths, iterators, callback):
        scanner = yield gen.Task(Scanner.create, self, table, scanrange, cols, auths, iterators)
        callback(scanner)

    @gen.engine
    def create_batch_writer(self, table, callback):
        bw = yield gen.Task(BatchWriter.create, self, table, **BW_DEFAULTS)
        callback(bw)

    @gen.engine
    def create_batch_writer_with_options(self, table, max_memory, latency_ms, timeout_ms, threads, callback):
        bw = yield gen.Task(BatchWriter.create, self, table, max_memory, latency_ms, timeout_ms, threads)
        callback(bw)