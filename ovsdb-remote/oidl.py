#---------------------------------------------#
# This code is based on Openstack's ovs-idl
#---------------------------------------------#

import collections
import subprocess
import os
import time
import uuid
import threading
import traceback
from ovs.db import idl
from ovs.db import schema
from ovs import jsonrpc
from ovs import poller
from ovs import stream
import ovs.poller
import retrying
from six.moves import queue as Queue
import oslo_i18n
from oslo_config import cfg

DOMAIN = "neutron"

_translators = oslo_i18n.TranslatorFactory(domain=DOMAIN)

# The primary translation function using the well-known name "_"
_ = _translators.primary

#from neutron._i18n import _


class SpirentException(Exception):
    """Base Neutron Exception.
    To correctly use this class, inherit from it and define
    a 'message' property. That message will get printf'd
    with the keyword arguments provided to the constructor.
    """
    message = _("An unknown exception occurred.")

    def __init__(self, **kwargs):
        super(SpirentException, self).__init__(self.message % kwargs)
        self.msg = self.message % kwargs

    def __str__(self):
        return self.msg


#----------- IDL-UTILS BEGIN --------------#

RowLookup = collections.namedtuple('RowLookup',
                                   ['table', 'column', 'uuid_column'])

# Tables with no index in OVSDB and special record lookup rules
_LOOKUP_TABLE = {
    'Controller': RowLookup('Bridge', 'name', 'controller'),
    'Flow_Table': RowLookup('Flow_Table', 'name', None),
    'IPFIX': RowLookup('Bridge', 'name', 'ipfix'),
    'Mirror': RowLookup('Mirror', 'name', None),
    'NetFlow': RowLookup('Bridge', 'name', 'netflow'),
    'QoS': RowLookup('Port', 'name', 'qos'),
    'Queue': RowLookup(None, None, None),
    'sFlow': RowLookup('Bridge', 'name', 'sflow'),
    'SSL': RowLookup('Open_vSwitch', None, 'ssl'),
}

_NO_DEFAULT = object()


class RowNotFound(SpirentException):
    message = _("Cannot find %(table)s with %(col)s=%(match)s")


def row_by_value(idl_, table, column, match, default=_NO_DEFAULT):
    """Lookup an IDL row in a table by column/value"""
    tab = idl_.tables[table]
    for r in tab.rows.values():
        if getattr(r, column) == match:
            return r
    if default is not _NO_DEFAULT:
        return default
    raise RowNotFound(table=table, col=column, match=match)


def row_by_record(idl_, table, record):
    t = idl_.tables[table]
    try:
        if isinstance(record, uuid.UUID):
            return t.rows[record]
        uuid_ = uuid.UUID(record)
        return t.rows[uuid_]
    except ValueError:
        # Not a UUID string, continue lookup by other means
        pass
    except KeyError:
        raise RowNotFound(table=table, col='uuid', match=record)

    rl = _LOOKUP_TABLE.get(table, RowLookup(table, get_index_column(t), None))
    # no table means uuid only, no column is just SSL which we don't need
    if rl.table is None:
        raise ValueError(_("Table %s can only be queried by UUID") % table)
    if rl.column is None:
        raise NotImplementedError(_("'.' searches are not implemented"))
    row = row_by_value(idl_, rl.table, rl.column, record)
    if rl.uuid_column:
        rows = getattr(row, rl.uuid_column)
        if len(rows) != 1:
            raise RowNotFound(table=table, col=_('record'), match=record)
        row = rows[0]
    return row


class ExceptionResult(object):

    def __init__(self, ex, tb):
        self.ex = ex
        self.tb = tb


def get_schema_helper(connection, schema_name):
    err, strm = stream.Stream.open_block(
        stream.Stream.open(connection))
    if err:
        print "Failure 1"
        raise Exception(_("Could not connect to %s") % connection)
    rpc = jsonrpc.Connection(strm)
    req = jsonrpc.Message.create_request('get_schema', [schema_name])
    err, resp = rpc.transact_block(req)
    rpc.close()
    if err:
        print "Failure 2"
        raise Exception(_("Could not retrieve schema from %(conn)s: "
                          "%(err)s") % {'conn': connection,
                                        'err': os.strerror(err)})
    elif resp.error:
        print "Failure 3"
        print resp.error
        raise Exception(resp.error)
    return idl.SchemaHelper(None, resp.result)


def wait_for_change(_idl, timeout, seqno=None):
    if seqno is None:
        seqno = _idl.change_seqno
    stop = time.time() + timeout
    while _idl.change_seqno == seqno and not _idl.run():
        ovs_poller = poller.Poller()
        _idl.wait(ovs_poller)
        ovs_poller.timer_wait(timeout * 1000)
        ovs_poller.block()
        if time.time() > stop:
            raise Exception(_("Timeout"))


def get_column_value(row, col):
    """Retrieve column value from the given row.

    If column's type is optional, the value will be returned as a single
    element instead of a list of length 1.
    """
    if col == '_uuid':
        val = row.uuid
    else:
        val = getattr(row, col)

    # Idl returns lists of Rows where ovs-vsctl returns lists of UUIDs
    if isinstance(val, list) and len(val):
        if isinstance(val[0], idl.Row):
            val = [v.uuid for v in val]
        col_type = row._table.columns[col].type
        # ovs-vsctl treats lists of 1 as single results
        if col_type.is_optional():
            val = val[0]
    return val


def condition_match(row, condition):
    """Return whether a condition matches a row

    :param row:       An OVSDB Row
    :param condition: A 3-tuple containing (column, operation, match)
    """
    col, op, match = condition
    val = get_column_value(row, col)

    # both match and val are primitive types, so type can be used for type
    # equality here.
    if type(match) is not type(val):
        # Types of 'val' and 'match' arguments MUST match in all cases with 2
        # exceptions:
        # - 'match' is an empty list and column's type is optional;
        # - 'value' is an empty and  column's type is optional
        if (not all([match, val]) and
                row._table.columns[col].type.is_optional()):
            # utilize the single elements comparison logic
            if match == []:
                match = None
            elif val == []:
                val = None
        else:
            # no need to process any further
            raise ValueError(
                _("Column type and condition operand do not match"))

    matched = True

    # TODO(twilson) Implement other operators and type comparisons
    # ovs_lib only uses dict '=' and '!=' searches for now
    if isinstance(match, dict):
        for key in match:
            if op == '=':
                if (key not in val or match[key] != val[key]):
                    matched = False
                    break
            elif op == '!=':
                if key not in val or match[key] == val[key]:
                    matched = False
                    break
            else:
                raise NotImplementedError()
    elif isinstance(match, list):
        # According to rfc7047, lists support '=' and '!='
        # (both strict and relaxed). Will follow twilson's dict comparison
        # and implement relaxed version (excludes/includes as per standard)
        if op == "=":
            if not all([val, match]):
                return val == match
            for elem in set(match):
                if elem not in val:
                    matched = False
                    break
        elif op == '!=':
            if not all([val, match]):
                return val != match
            for elem in set(match):
                if elem in val:
                    matched = False
                    break
        else:
            raise NotImplementedError()
    else:
        if op == '=':
            if val != match:
                matched = False
        elif op == '!=':
            if val == match:
                matched = False
        else:
            raise NotImplementedError()
    return matched


def row_match(row, conditions):
    """Return whether the row matches the list of conditions"""
    return all(condition_match(row, cond) for cond in conditions)


def get_index_column(table):
    if len(table.indexes) == 1:
        idx = table.indexes[0]
        if len(idx) == 1:
            return idx[0].name

#----------- IDL-UTILS END ----------------#

#----------- CONNECTION HELPER ----------------#


def _connection_to_manager_uri(conn_uri):
    proto, addr = conn_uri.split(':', 1)
    if ':' in addr:
        ip, port = addr.split(':', 1)
        return 'p%s:%s:%s' % (proto, port, ip)
    else:
        return 'p%s:%s' % (proto, addr)


def enable_connection_uri():
    manager_uri = _connection_to_manager_uri(CONF.OVS.ovsdb_connection)
    subprocess.Popen(['ovs-vsctl', 'set-manager', manager_uri], stdin=None,
                     stderr=None)

#----------- HELPERS END ------------------#
OPTS = [
    cfg.StrOpt('ovsdb_connection',
               default='tcp:127.0.0.1:6640',
               help=_('The connection string for the native OVSDB backend. '
                      'Requires the native ovsdb_interface to be enabled.')),
    cfg.StrOpt('schema_file',
               default='/usr/share/openvswitch/vswitch.ovsschema',
               help=_('The complete path of the openvswitch db schema'))
]

CONF = cfg.CONF
CONF.register_opts(OPTS, 'OVS')

class TransactionQueue(Queue.Queue, object):

    def __init__(self, *args, **kwargs):
        super(TransactionQueue, self).__init__(*args, **kwargs)
        alertpipe = os.pipe()
        # NOTE(ivasilevskaya) python 3 doesn't allow unbuffered I/O. Will get
        # around this constraint by using binary mode.
        self.alertin = os.fdopen(alertpipe[0], 'rb', 0)
        self.alertout = os.fdopen(alertpipe[1], 'wb', 0)

    def get_nowait(self, *args, **kwargs):
        try:
            result = super(TransactionQueue, self).get_nowait(*args, **kwargs)
        except Queue.Empty:
            return None
        self.alertin.read(1)
        return result

    def put(self, *args, **kwargs):
        super(TransactionQueue, self).put(*args, **kwargs)
        self.alertout.write('X')
        self.alertout.flush()

    @property
    def alert_fileno(self):
        return self.alertin.fileno()


class Connection(object):

    def __init__(self, connection, timeout, schema_name):
        self.idl = None
        self.connection = connection
        self.timeout = timeout
        self.txns = TransactionQueue(1)
        self.lock = threading.Lock()
        self.schema_name = schema_name

    def start(self, table_name_list=None):
        """
        :param table_name_list: A list of table names for schema_helper to
                register. When this parameter is given, schema_helper will only
                register tables which name are in list. Otherwise,
                schema_helper will register all tables for given schema_name as
                default.
        """
        with self.lock:
            if self.idl is not None:
                return

            try:
                helper = get_schema_helper(self.connection,
                                           self.schema_name)
            except Exception:
                # We may have failed do to set-manager not being called
                enable_connection_uri()

                # There is a small window for a race, so retry up to a second
                @retrying.retry(wait_exponential_multiplier=10,
                                stop_max_delay=1000)
                def do_get_schema_helper():
                    return get_schema_helper(self.connection,
                                             self.schema_name)
                helper = do_get_schema_helper()

            if table_name_list is None:
                helper.register_all()
            else:
                for table_name in table_name_list:
                    helper.register_table(table_name)
            self.idl = idl.Idl(self.connection, helper)
            wait_for_change(self.idl, self.timeout)
            #self.poller = poller.Poller()
            #self.thread = threading.Thread(target=self.run)
            # self.thread.setDaemon(True)
            # self.thread.start()

    def run(self):
        while True:
            self.idl.wait(self.poller)
            self.poller.fd_wait(self.txns.alert_fileno, poller.POLLIN)
            # TODO(jlibosva): Remove next line once losing connection to ovsdb
            #                is solved.
            self.poller.timer_wait(self.timeout * 1000)
            self.poller.block()
            self.idl.run()
            txn = self.txns.get_nowait()
            if txn is not None:
                try:
                    txn.results.put(txn.do_commit())
                except Exception as ex:
                    er = ExceptionResult(ex=ex,
                                         tb=traceback.format_exc())
                    txn.results.put(er)
                self.txns.task_done()

    def queue_txn(self, txn):
        self.txns.put(txn)


class OvsdbIdl():

    def __init__(self):
        self.timeout = 10
        self.ovsdb = Connection(CONF.OVS.ovsdb_connection,
                                self.timeout,
                                'Open_vSwitch')
        self.ovsdb.start()
        self.idl = self.ovsdb.idl

        # try:
        #    helper =
        #helper = idl.SchemaHelper(CONF.OVS.schema_file)
        # helper.register_table("Bridge")
        # helper.register_table("Open_vSwitch")
        #    helper.register_all()
        #    self.idl = idl.Idl(CONF.OVS.ovsdb_connection, helper)
        #    self.idl.run()
        # except Exception:
        #    print ("FAILED")
        #    # We may have failed do to set-manager not being called
        #    enable_connection_uri()
        #    sys.exit()

    @property
    def _tables(self):
        return self.idl.tables

    @property
    def _ovs(self):
        return list(self._tables['Open_vSwitch'].rows.values())[0]

    def list_interfaces(self):
        # self.idl.run()
        bridges = [x.name for x in
                   self._tables['Bridge'].rows.values()]
        for bridge in bridges:
            br = row_by_value(self.idl, 'Bridge',
                              'name', bridge)
            #ports = [p.name for p in br.ports if p.name != bridge]
            interfaces = [i for p in br.ports if p.name != bridge
                          for i in p.interfaces]
            for iface in interfaces:
                if iface.external_ids and 'vm-uuid' in iface.external_ids:
                    print("%s %s %s %s %s" % (iface.name, iface.mac,
                                              iface.mac_in_use,
                                              iface.external_ids[
                                                  'attached-mac'],
                                              iface.external_ids['vm-uuid']))
                print "----------------------------------"
                print "Interface name:", iface.name
                print "Interface MAC:", iface.mac
                print "****** Statistics *********"
                print iface.mac_in_use
                print iface.statistics
                print iface.external_ids


def main():
    #remote = 'ptcp:127.0.0.1:6640'
    print CONF.OVS.ovsdb_connection
    print CONF.OVS.schema_file
    ovsidl = OvsdbIdl()
    ovsidl.list_interfaces()

if __name__ == '__main__':
    CONF(default_config_files=['settings.conf'])
    main()
