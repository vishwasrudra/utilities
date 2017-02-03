import os
import libvirt
from xml.dom import minidom


def mapInterfacesToInstances(interfaces):
    conn = libvirt.open('qemu:///system')
    if conn == None:
        print('Failed to open connection to qemu:///system')
        exit(1)

    domainIDs = conn.listDomainsID()
    if domainIDs == None:
        print('Failed to get a list of domain IDs')
        exit(1)

    for domainID in domainIDs:
        domain = conn.lookupByID(domainID)
        if domain == None:
            print('Failed to find the domain %s', domainID)
            exit(1)
        raw_xml = domain.XMLDesc(0)
        xml = minidom.parseString(raw_xml)
        interfaceTypes = xml.getElementsByTagName('interface')
        for interfaceType in interfaceTypes:
            interfaceNodes = interfaceType.childNodes
            for interfaceNode in interfaceNodes:
                if interfaceNode.nodeName[0:1] != '#':
                    for attr in interfaceNode.attributes.keys():
                        if interfaceNode.attributes[attr].name == 'path':
                            i = os.path.basename(interfaceNode.
                                                 attributes[attr].value)
                            if i in interfaces:
                                interfaceMap[i]=domain.name()
                        elif interfaceNode.attributes[attr].name == 'dev':
                            i = interfaceNode.attributes[attr].value
                            i = i.replace("tap", "qvo")
                            if i in interfaces:
                                interfaceMap[i]=domain.name()
                        elif interfaceNode.attributes[attr].name == 'bridge':
                            i = interfaceNode.attributes[attr].value
                            i = i.replace("qbr", "qvo")
                            if i in interfaces:
                                interfaceMap[i]=domain.name()
    conn.close()

def getInterfaces():
    files = [f for f in os.listdir("/var/run/openvswitch")]
    interfaces = []
    for fi in files:
        if fi.startswith('qvo') or fi.startswith('vhu'):
            interfaces.append(fi)
    return interfaces

interfaceMap = {}

def printInterfaceMap():
    print "{:<25} {:<25}".format('Interface','Instance')
    for k, v in interfaceMap.iteritems():
        print "{:<25} {:<25}".format(k, v)

def main():
    ifaces = getInterfaces()
    mapInterfacesToInstances(ifaces)
    printInterfaceMap()


if __name__ == "__main__":
    main()

