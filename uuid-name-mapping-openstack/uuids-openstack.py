
# The UUIDs to consider:
#instance-id
#net-id
#port-id
#[[Cinder]]
#[[Glance]]
#[[Manila]]
#[[Swift]]
#[[Barbican]]
#[[Ceilometer]]
#[[Heat]]
#[[Trove]]

# Defer client import until we actually need them
import argparse
import socket
import string
import struct
import sys
import time
import re
import uuid

import zmq
import sqlite3

from novaclient import client as novaclient
from cinderclient import client as cinderclient
from keystoneauth1 import identity
from keystoneauth1 import session
from keystoneauth1.identity import v2
from keystoneclient.v2_0 import client as idclient
from neutronclient.v2_0 import client as neutronclient
from oslo_config import cfg

opt_credentials_group = cfg.OptGroup(name='CREDENTIALS',
                                     title='Openstack Credentials')
credential_opts = [cfg.StrOpt('username', default='admin'),
                   cfg.StrOpt('password', default='admin'),
                   cfg.StrOpt('tenant', default='admin'),
                   cfg.StrOpt('url',
                              default='http://10.140.8.129:5000/v2.0')
                   ]

cli_opts = [
    cfg.StrOpt('uuid', default='', required=True),
    cfg.StrOpt('name', default='', required=True),
    cfg.StrOpt('resource', default='', required=True),
    cfg.StrOpt('value', default='', required=True),
    cfg.StrOpt('output', default='', required=True)
    ]

CONF = cfg.CONF
CONF.register_group(opt_credentials_group)
CONF.register_opts(credential_opts, opt_credentials_group)
CONF.register_cli_opts(cli_opts)

#def get_name_from_uuid(uuid, project_name):



#def get_uuid_from_name()
class OpenstackClient():

    def __init__(self):
        self.db = sqlite3.connect(':memory:')
        self.cur = self.db.cursor()
        self.init_db()
        self.user = CONF.CREDENTIALS.username
        self.pwd = CONF.CREDENTIALS.password
        self.url = CONF.CREDENTIALS.url
        self.tenant = CONF.CREDENTIALS.tenant
        self.novc = novaclient.Client(2, self.user, self.pwd, self.tenant,
                                      auth_url=self.url)
        self.cinc = cinderclient.Client('2', self.user, self.pwd, self.tenant,
                                        auth_url=self.url)

        self.auth = identity.Password(auth_url=self.url,
                             username=self.user,
                             password=self.pwd,
                             project_name=self.tenant)
        self.sess = session.Session(auth=self.auth)
        self.neuc = neutronclient.Client(session=self.sess)
        self.keystone = idclient.Client(session=self.sess)
        #self.neuc = neutronclient.Client('2',self.user, self.pwd, self.tenant,
        #                                 auth_url=self.url)


    def populate_db(self, uuid, name, context):
        self.cur.execute('''INSERT INTO osuuid (Uuid, Name, Context)
                         VALUES (?, ?, ?)''', (uuid, name, context))
        self.db.commit()

    def init_db(self):
        self.cur.execute('''CREATE TABLE osuuid (
            Uuid TEXT, Name TEXT, Context TEXT)''')

    def print_db(self):
        output = self.cur.execute('''SELECT Uuid, Name, Context FROM
                                  osuuid''')
        for row in output:
            print "UUID:", row[0]
            print "NAME:", row[1],
            print "CONTEXT:", row[2]


    def print_values(self, val, type):
        if type == 'ports':
            val_list = val['ports']
        if type == 'networks':
            val_list = val['networks']
        if type == 'routers':
            val_list = val['routers']
        for p in val_list:
            for k, v in p.items():
                print("%s : %s" % (k, v))
            print('\n')

    def print_values_server(val, server_id, type):
        if type == 'ports':
            val_list = val['ports']

        if type == 'networks':
            val_list = val['networks']
        for p in val_list:
            bool = False
            for k, v in p.items():
                if k == 'device_id' and v == server_id:
                    bool = True
            if bool:
                for k, v in p.items():
                    print("%s : %s" % (k, v))
                print('\n')

    def print_nova_info(self):
        for server in self.novc.servers.list():
            print "------------------------------------------"
            #print server._info
            print "------------------------------------------"
            print "Name:", server._info['name']
            print "ID", server._info['id']
            print "User-ID", server._info['user_id']
            print "HostId", server._info['hostId']
            print "Image-Id", server._info['image']['id']
            print "Flavor-Id", server._info['flavor']['id']
            print "Tenant-Id", server._info['tenant_id']
            for l in range(len(server._info[
                'os-extended-volumes:volumes_attached'])):
                print "Volume-ID", server._info[
                    'os-extended-volumes:volumes_attached'][l]['id']
            self.populate_db(str(server._info['id']),
                             str(server._info['name']),
                             'Server')


    def print_neutron_info(self):
        net_list=self.neuc.list_networks()
        nws = net_list['networks']
        print "----- PRINTING NETWORKS -----"
        for nw in nws:
            print"********************************"
            print nw
            print"********************************"
            for n in range(len(nw['subnets'])):
                print "Subnet", nw['subnets'][n]
            print "Tenant-ID:", nw['tenant_id']
            print "Network ID:", nw['id']
        subnet_list=self.neuc.list_subnets()
        snets = subnet_list['subnets']
        print "----- PRINTING SUBNETS -----"
        for subnet in snets:
            print"********************************"
            print subnet
            print"********************************"
            print "Subnet name", subnet['name']
            print "Subnet ID", subnet['id']
        ports_list = self.neuc.list_ports()
        ports = ports_list['ports']
        print "----- PRINTING PORTS -----"
        for port in ports:
            print"********************************"
            print port
            print"********************************"
            print "Port ID:", port['id']
            print "Tenant ID:", port['tenant_id']
            print "Network ID:", port['network_id']
            for p in range(len(port['fixed_ips'])):
                print "Subnet id:", port['fixed_ips'][p]['subnet_id']
            print "Device ID:", port['device_id']

        routers_list = self.neuc.list_routers(retrieve_all=True)
        routers = routers_list['routers']
        print "----- PRINTING ROUTERS -----"
        for router in routers:
            print"********************************"
            print router
            print"********************************"
            if router['external_gateway_info'] != None:
                print("External Gateway N/w ID",
                      router['external_gateway_info']['network_id'])
                print("Number of external fixed ips", len(
                    router['external_gateway_info'][
                          'external_fixed_ips']))
                for x in range(len(router['external_gateway_info'][
                    'external_fixed_ips'])):
                    print("External Fixed IP Subnet ID",
                          router['external_gateway_info'][
                              'external_fixed_ips'][x]['subnet_id'])
            print("Router Id", router['id'])
            print("Tenant Id", router['tenant_id'])

    def list_uuids(self):
        #services= self.keystone.services.list()
        #print services
        for server in self.novc.servers.list():
            print "------------------------------------------"
            print server._info
            print "------------------------------------------"
        #self.print_db()



    def get_name_from_uuid_resource(self):
        uuid = CONF.uuid
        resource = CONF.resource
        if resource == 'instance':
            for server in self.novc.servers.list():
                #print server._info['id']
                if server._info['id'] == uuid:
                    return server._info['name']
        elif resource == 'network':
            net_list=self.neuc.list_networks()
            networks = net_list['networks']
            for network in networks:
                if network['id'] == uuid:
                    return network['name']
        elif resource == 'subnet':
            subnet_list=self.neuc.list_subnets()
            snets = subnet_list['subnets']
            for subnet in snets:
                if subnet['id'] == uuid:
                    return subnet['name']
        elif resource == 'port':
            ports_list = self.neuc.list_ports()
            ports = ports_list['ports']
            for port in ports:
                if port['id'] == uuid:
                    return port['name']
        elif resource == 'router':
            routers_list = self.neuc.list_routers(retrieve_all=True)
            routers = routers_list['routers']
            for router in routers:
                if router['id'] == uuid:
                    return router['name']

    def get_uuid_from_name_resource(self):
        resource = CONF.resource
        name = CONF.name
        if resource == 'instance':
            for server in self.novc.servers.list():
                if server._info['name'] == name:
                    return server._info['id']
        elif resource == 'network':
            net_list=self.neuc.list_networks()
            networks = net_list['networks']
            for network in networks:
                if network['name'] == name:
                    return network['id']
        elif resource == 'subnet':
            subnet_list=self.neuc.list_subnets()
            snets = subnet_list['subnets']
            for subnet in snets:
                if subnet['name'] == name:
                    return subnet['id']
        elif resource == 'port':
            ports_list = self.neuc.list_ports()
            ports = ports_list['ports']
            for port in ports:
                if port['name'] == name:
                    return port['id']
        elif resource == 'router':
            routers_list = self.neuc.list_routers(retrieve_all=True)
            routers = routers_list['routers']
            for router in routers:
                if router['name'] == name:
                    return router['id']
    #def get_uuid_from_name(self):

    def get_network_info_from_mac(self):
        nw_info = {}
        mac_address = CONF.value
        ports_list = self.neuc.list_ports()
        ports = ports_list['ports']
        for port in ports:
            if port['mac_address'] == mac_address:
                nw_info['net_id'] = port['network_id']
                if len(port['fixed_ips']) > 0:
                    nw_info['subnet_id'] = port['fixed_ips'][0]['subnet_id']
                else:
                    nw_info['subnet_id'] = ''
                nw_info['port_name'] = port['name']

        if (nw_info.get('net_id') == None):
            print "FAILURE"
            return nw_info
        subnet_list=self.neuc.list_subnets()
        snets = subnet_list['subnets']
        net_list=self.neuc.list_networks()
        nws = net_list['networks']
        for nw in nws:
            if nw['id'] == nw_info['net_id']:
                nw_info['net_name'] = nw['name']
                break
        for sub in snets:
            if sub['id'] == nw_info['subnet_id']:
                nw_info['subnet_name'] = sub['name']
                break
        return nw_info


def main():
    client = OpenstackClient()
    if (CONF.uuid != '' and CONF.resource != ''):
        print (client.get_name_from_uuid_resource())
    if (CONF.name != '' and CONF.resource != ''):
        print (client.get_uuid_from_name_resource())
    if (CONF.resource != '' and CONF.value != ''):
        print "RESOLVING MAC"
        nw_info = client.get_network_info_from_mac()
        print nw_info
    else:
        client.print_neutron_info()

if __name__ == '__main__':
    CONF(default_config_files=['settings.conf'])
    main()
