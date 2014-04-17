from time import time
import tornado.gen
import tornado.testing
from pyaccumulo import Mutation, Range
from pyaccumulo.tornado import Accumulo
from pyaccumulo.tornado.proxy.ttypes import TableExistsException, AccumuloSecurityException, TableNotFoundException, \
    UnknownScanner, UnknownWriter


_ac_conn = None


# update with your accumulo connections
ACC_CONN = dict(
    host="localhost",
    port=42424,
    user="user",
    password="pass"
)

METADATA_TABLE = "!METADATA"  # true for Accumulo 1.5
TEMP_TABLE = ('unittest_%f_' % time()).replace('.', '_') + "temp"


class AccumuloTest(tornado.testing.AsyncTestCase):
    """
    Why not gen.coroutine? Trying to keep it compatible with Tornado 2.x since the thrift files are generated in the
    same way...
    """
    @tornado.gen.engine
    def _get_connection(self, callback):
        if globals()["_ac_conn"] is None:
            globals()["_ac_conn"] = yield tornado.gen.Task(Accumulo.create_and_connect, **ACC_CONN)
        callback(globals()["_ac_conn"])

    def _assert_raises(self, exc_class, func, *args, **kwargs):
        """
        The assertRaises function doesn't work correctly for our async test cases...use this instead
        """
        try:
            if "callback" not in kwargs:
                kwargs["callback"] = self.stop
            func(*args, **kwargs)
            self.wait()
        except Exception as ex:
            self.assertIsInstance(ex, exc_class)
        else:
            self.assertTrue(False, "Expected an exception of type '%s', but received none." %
                                   exc_class().__class__.__name__)

    def setUp(self):
        super(AccumuloTest, self).setUp()

    def tearDown(self):
        super(AccumuloTest, self).tearDown()
        if globals()["_ac_conn"] is not None:
            globals()["_ac_conn"].close()
            globals()["_ac_conn"] = None

    def test_bad_creds(self):
        self._assert_raises(AccumuloSecurityException, Accumulo.create_and_connect, host=ACC_CONN["host"],
                            port=ACC_CONN["port"], user="notauser", password="notapassword")

    def test_list_tables(self):
        self._get_connection(callback=self.stop)
        conn = self.wait()
        conn.list_tables(callback=self.stop)
        tables = self.wait()
        self.assertIn(METADATA_TABLE, tables)

    def test_table_exists(self):
        self._get_connection(callback=self.stop)
        conn = self.wait()
        conn.table_exists(METADATA_TABLE, callback=self.stop)
        res = self.wait()
        self.assertTrue(res)
        conn.table_exists(TEMP_TABLE, callback=self.stop)
        res = self.wait()
        self.assertFalse(res)

    def test_create_table(self):
        """
        NOTE: table created by this call gets deleted by the function below - this works since tests are ordered
              by their name!!! 'test_c' comes before 'test_d' so this is why this works nicely.
        """
        self._get_connection(callback=self.stop)
        conn = self.wait()
        conn.create_table(TEMP_TABLE, callback=self.stop)
        self.wait()
        conn.table_exists(TEMP_TABLE, callback=self.stop)
        res = self.wait()
        self.assertTrue(res)
        # make sure an exception is raised if we try to create it again
        self._assert_raises(TableExistsException, conn.create_table, TEMP_TABLE)

    def test_delete_table(self):
        self._get_connection(callback=self.stop)
        conn = self.wait()
        conn.delete_table(TEMP_TABLE, callback=self.stop)
        self.wait()
        conn.table_exists(TEMP_TABLE, callback=self.stop)
        res = self.wait()
        self.assertFalse(res)
        # make sure an exception is raised if we try to delete a table that no longer exists
        self._assert_raises(TableNotFoundException, conn.delete_table, TEMP_TABLE)

    def test_rename_table(self):
        self._get_connection(callback=self.stop)
        conn = self.wait()
        conn.create_table(TEMP_TABLE, callback=self.stop)
        self.wait()
        conn.rename_table(TEMP_TABLE, TEMP_TABLE + "_renamed", callback=self.stop)
        self.wait()
        # make sure an exception is raised if we try to rename the table to a table that already exists
        conn.create_table(TEMP_TABLE, callback=self.stop)
        self.wait()
        self._assert_raises(TableExistsException, conn.rename_table, TEMP_TABLE, TEMP_TABLE + "_renamed")
        # delete the tables...
        conn.delete_table(TEMP_TABLE, callback=self.stop)
        self.wait()
        conn.delete_table(TEMP_TABLE + "_renamed", callback=self.stop)
        self.wait()
        # make sure an exception is raised if we try to rename a table that no longer exists
        self._assert_raises(TableNotFoundException, conn.rename_table, TEMP_TABLE, TEMP_TABLE + "_renamed")

    def test_read_and_write(self):
        self._get_connection(callback=self.stop)
        conn = self.wait()
        conn.create_table(TEMP_TABLE, callback=self.stop)
        self.wait()
        conn.create_batch_writer(TEMP_TABLE, callback=self.stop)
        bw = self.wait()
        self.assertFalse(bw._is_closed)

        # write some data - one mutation at a time
        for i in xrange(50):
            mut = Mutation("%02d" % i)
            for j in xrange(5):
                mut.put(cf="family%02d" % j, cq="qualifier%02d" % j, val="%02d" % j)
            bw.add_mutation(mut)
        bw.flush(callback=self.stop)
        self.wait()

        # write some more data - this time add all mutations simultaenously
        muts = []
        for i in xrange(50, 90):
            mut = Mutation("%02d" % i)
            for j in xrange(5):
                mut.put(cf="family%02d" % j, cq="qualifier%02d" % j, val="%02d" % j)
            muts.append(mut)
        bw.add_mutations(muts)
        bw.flush(callback=self.stop)
        self.wait()

        mut = Mutation("")
        # TODO: adding a bad mutation causes an AttributeError - not a MutationsRejectedException!!!
        # a bad mutation causes the proxy to send a request back that the library doesn't handle!!!

        # we are done with the batchwriter, so shut it down
        bw.close(callback=self.stop)
        self.wait()

        # make sure an exception is raised if we try to right to a closed batch writer
        self.assertRaises(UnknownWriter, bw.add_mutation, mut)

        # write some more data - this time add some mutations without creating a batchwriter
        muts = []
        for i in xrange(90, 100):
            mut = Mutation("%02d" % i)
            for j in xrange(5):
                mut.put(cf="family%02d" % j, cq="qualifier%02d" % j, val="%02d" % j)
            muts.append(mut)
        conn.add_mutations(TEMP_TABLE, muts, callback=self.stop)
        self.wait()

        # read the data back
        conn.create_scanner(TEMP_TABLE, callback=self.stop)
        scanner = self.wait()
        all_entries = []
        while scanner.has_next():
            scanner.next(callback=self.stop)
            entries = self.wait()
            all_entries.extend(entries)
        # make sure there are no more entries...
        # NOTE: I expected this to throw a NoMoreEntriesException, but it does not...
        scanner.next(callback=self.stop)
        entries = self.wait()
        self.assertEqual(0, len(entries))
        scanner.close(callback=self.stop)
        self.wait()
        # make sure it throws an exception if we try to read from a closed scanner or close a closed scanner
        self._assert_raises(UnknownScanner, scanner.next)
        self._assert_raises(UnknownScanner, scanner.close)

        # 100 mutations with 5 entries per
        self.assertEqual(len(all_entries), 500)
        self._assert_entries(all_entries, start=0, end=99)

        # create a scanner with a range - its inclusive, so this is 30 logical rows: 54-25+1
        conn.create_scanner(TEMP_TABLE, scanrange=Range(srow="25", erow="54"), callback=self.stop)
        scanner = self.wait()
        all_entries = []
        while scanner.has_next():
            scanner.next(callback=self.stop)
            entries = self.wait()
            all_entries.extend(entries)
        # make sure there are no more entries...
        # NOTE: I expected this to throw a NoMoreEntriesException, but it does not...
        scanner.next(callback=self.stop)
        entries = self.wait()
        self.assertEqual(0, len(entries))
        scanner.close(callback=self.stop)
        self.wait()
        # make sure it throws an exception if we try to read from a closed scanner or close a closed scanner
        self._assert_raises(UnknownScanner, scanner.next)
        self._assert_raises(UnknownScanner, scanner.close)

        # 30 mutations with 5 entries per
        self.assertEqual(len(all_entries), 150)
        self._assert_entries(all_entries, start=25, end=54)

        # create a scanner to filter columns - just choose one logical row
        conn.create_scanner(TEMP_TABLE, scanrange=Range(srow="50", erow="50"),
                            cols=[["family03", "qualifier03"], ["family01", "qualifier01"]], callback=self.stop)
        scanner = self.wait()
        all_entries = []
        while scanner.has_next():
            scanner.next(callback=self.stop)
            entries = self.wait()
            all_entries.extend(entries)
        # make sure there are no more entries...
        # NOTE: I expected this to throw a NoMoreEntriesException, but it does not...
        scanner.next(callback=self.stop)
        entries = self.wait()
        self.assertEqual(0, len(entries))
        scanner.close(callback=self.stop)
        self.wait()
        # make sure it throws an exception if we try to read from a closed scanner or close a closed scanner
        self._assert_raises(UnknownScanner, scanner.next)
        self._assert_raises(UnknownScanner, scanner.close)

        # make sure we only got a single logical row with 2 entries
        self.assertEqual(len(all_entries), 2)
        self.assertEqual(all_entries[0].row, "50")
        self.assertEqual(all_entries[0].cf, "family01")
        self.assertEqual(all_entries[0].cq, "qualifier01")
        self.assertEqual(all_entries[0].val, "01")
        self.assertEqual(all_entries[1].row, "50")
        self.assertEqual(all_entries[1].cf, "family03")
        self.assertEqual(all_entries[1].cq, "qualifier03")
        self.assertEqual(all_entries[1].val, "03")

        # let's do a batch scan and make sure that works too
        scanranges = [Range(srow="00", erow="00"), Range(srow="50", erow="50"), Range(srow="75", erow="75")]
        conn.create_batch_scanner(TEMP_TABLE, scanranges=scanranges, callback=self.stop)
        scanner = self.wait()
        all_entries = []
        while scanner.has_next():
            scanner.next(callback=self.stop)
            entries = self.wait()
            all_entries.extend(entries)
        scanner.close(callback=self.stop)
        self.wait()
        # make sure it throws an exception if we try to read from a closed scanner or close a closed scanner
        self._assert_raises(UnknownScanner, scanner.next)
        self._assert_raises(UnknownScanner, scanner.close)

        # 3 mutations with 5 entries per
        self.assertEqual(len(all_entries), 15)

        # no guarantee the entries are sorted, so let's sort them
        all_entries = sorted(all_entries, key=lambda x: x.row)
        self.assertEqual(all_entries[0].row, "00")
        self.assertEqual(all_entries[5].row, "50")
        self.assertEqual(all_entries[10].row, "75")

        conn.delete_table(TEMP_TABLE, callback=self.stop)
        self.wait()

        # make sure an exception is raised if we try to create a writer to a non-existent table
        self._assert_raises(TableNotFoundException, conn.create_batch_writer, TEMP_TABLE)

        # do the same for a scanner...
        self._assert_raises(TableNotFoundException, conn.create_scanner, TEMP_TABLE)

        # ...and a batch scanner
        self._assert_raises(TableNotFoundException, conn.create_batch_scanner, TEMP_TABLE, scanranges=scanranges)

    def _assert_entries(self, all_entries, start=0, end=0):
        i = start
        j = 0
        for entry in all_entries:
            self.assertEqual(entry.row, "%02d" % i)
            self.assertEqual(entry.cf, "family%02d" % j)
            self.assertEqual(entry.cq, "qualifier%02d" % j)
            self.assertEqual(entry.val, "%02d" % j)
            j += 1
            if j == 5:
                i += 1
                j = 0
        # make sure we got what the scan expected
        if end > start:
            self.assertEqual(all_entries[-1].row, "%02d" % end)