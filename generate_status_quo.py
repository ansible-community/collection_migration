#!/usr/bin/env python3

# generate_status_quo.py - directly map current module folders to collections
#
# examples:
#   cloud/amazon -> cloud.amazon
#   packaging/os -> packaging.os
#
# Any plugins which can not be easily mapped will end up in an _orphaned file.
# Ideally, -you- munge this script until no files are orphaned.


import contextlib
import copy
import os
import shutil
import subprocess

from logzero import logger
import ruamel.yaml
from sh import git
from sh import find



class StatusQuo:

    collections = None
    plugins = None
    pluginfiles = None
    orphaned = None
    topics = None

    synonyms = {
        'alicloud_ecs': 'alicloud',
        'apache-libcloud': 'cloud.misc',
        'aws': 'amazon',
        #'azure_rm': 'azure',
        'bigip': 'f5',
        'bigiq': 'f5',
        'buildah': 'podman',
        #'ce': 'cloudengine',
        'checkpoint': 'check_point',
        'cloudforms': 'cloud',
        #'cloudstack': 'cloud.cloudstack',
        #'consul': 'clustering.consul',
        'conjur': 'cloud',
        'credstash': 'cloud',
        'csharp': 'windows',
        'cyberarkpassword': 'cyberark',
        'dig': 'net_tools',
        'dnstext': 'net_tools',
        'ec2': 'amazon',
        #'ecs': 'crypto.entrust',
        'etcd': 'cloud',
        #'kube': 'k8s',
        #'foreman': 'remote_management.foreman',
        'spacewalk': 'remote_management',
        'f5_utils': 'f5',
        'gcdns': 'google',
        'gce': 'google',
        'gcp': 'google',
        'hashi_vault': 'identity',
        'chef_databag': 'identity',
        'lastpass': 'identity',
        'logdna': 'monitoring',
        'hetzner': 'hcloud',
        'hiera': 'puppet',
        'hwc': 'huawei',
        'httpapi': 'network',
        #'jabber': 'web_infrastructure',
        'infinibox': 'infinidat',
        #'infoblox': 'net_tools.nios',
        'infoblox': 'nios',
        #'ipa': 'identity.ipa',
        'ipaddr': 'network',
        'kubectl': 'k8s',
        'libcloud': 'cloud.misc',
        #'linode': 'cloud.linode',
        'logstash': 'monitoring',
        #'lxc': 'cloud.lxc',
        #'lxd': 'cloud.lxc',

        #'mso': 'network.aci',
        'mso': 'aci',
        #'nagios': 'monitoring.nagios',
        #'nagios': 'web_infrastructure',
        'nrdp': 'monitoring',
        'napalm': 'network',
        'oc': 'k8s',
        #'openstack': 'cloud.openstack',
        #'openshift': 'clustering.openshift',
        'openswitch': 'opx',
        #'openvz': 'cloud.misc',
        #'ovirt': 'cloud.ovirt',
        'onepassword': 'identity',

        #'package': 'packaging',
        'package': 'packaging.os',
        'paramiko': 'network',
        'passwordstore': 'identity',

        'postgres': 'postgresql',
        'powershell': 'windows',
        #'proxmox': 'cloud.misc',
        'psrp': 'windows',
        'qubes': 'cloud',
        'rax': 'cloud.rackspace',
        #'redis': 'database.misc',
        'rhv': 'ovirt',
        'sumologic': 'monitoring',
        'tower': 'ansible_tower',
        #'utm': 'sophos_utm',
        'vagrant': 'cloud',
        'vca': 'vmware',
        #'virtualbox': 'cloud.misc',
        #'vbox': 'cloud.misc',

        'win': 'windows',
        'win': 'windows',

        'Vmware': 'vmware',
        'vmware': 'cloud.vmware',
        #'yum': 'packaging.os',
        #'zabbix': 'monitoring.zabbix'
        'yumdnf': 'packaging',
    }

    def __init__(self):
        self.pluginfiles = []
        self.collections = {}
        self.url = 'https://github.com/ansible/ansible'
        self.checkouts_dir = '.cache/checkouts'
        self.checkout_dir = os.path.join(self.checkouts_dir, 'ansible')

    @classmethod
    def run(cls):
        self = cls()
        self.manage_checkout()
        self.get_plugins()
        self.make_spec()

    def manage_checkout(self):
        logger.info('manage checkout')

        if not os.path.exists(self.checkouts_dir):
            os.makedirs(self.checkouts_dir)

        if not os.path.exists(self.checkout_dir):
            logger.info('git clone %s %s' % (self.url, self.checkout_dir))
            git.clone(self.url, self.checkout_dir)
        else:
            logger.info('git fetch -a')
            git.fetch('-a', _cwd=self.checkout_dir)
            logger.info('git pull --rebase')
            git.pull('--rebase', _cwd=self.checkout_dir)

    def _guess_topic(self, filename):
        bn = os.path.basename(filename)
        bn = bn.replace('.py', '').replace('.ini', '')

        # workaround for everything depending on aws
        if bn == 'core' and 'aws' not in filename:
            return 'utilities.misc'

        # does the filepath contain a topic?
        paths = filename.replace(self.checkout_dir + '/', '')
        paths = paths.replace('lib/ansible/', '')
        paths = paths.split('/')
        paths = paths[1:]

        for ltup in zip(paths, paths[1:]):
            thistopic = '.'.join(ltup)
            if thistopic in self.topics:
                return thistopic

        # match on similar filenames
        for pf in self.pluginfiles:
            if not pf[2]:
                continue
            if os.path.basename(pf[-1]).replace('.py', '') == bn:
                logger.debug('A. %s --> %s' % (filename, pf[2]))
                #if 'fortios' in filename:
                #    import epdb; epdb.st()
                return pf[2]

        # match basename to similar dirname
        for pf in self.pluginfiles:
            if not pf[2]:
                continue
            xdn = os.path.basename(os.path.dirname(pf[-1]))
            if xdn == bn:
                logger.debug('A(1). %s --> %s' % (filename, pf[2]))
                #if 'fortios' in filename:
                #    import epdb; epdb.st()
                return pf[2]

        '''
        # match similar dirname to similar dirname
        fdn = os.path.basename(os.path.dirname(filename))
        for pf in self.pluginfiles:
            if not pf[2]:
                continue
            xdn = os.path.basename(os.path.dirname(pf[-1]))
            if fdn == xdn:
                logger.debug('A(2). %s --> %s' % (filename, pf[2]))
                return pf[2]
        '''

        # use path segments to match
        fparts = filename.split('/')
        fparts[-1] = fparts[-1].split('.')[0]
        for part in fparts[::-1]:
            if part in self.topics:
                return part
        for part in fparts[::-1]:
            for topic in self.topics:
                if not '.' in topic:
                    continue
                if topic.startswith(part + '.') or topic.endswith('.' + part):

                    #if 'fortios' in filename:
                    #    import epdb; epdb.st()

                    return topic
        for part in fparts[::-1]:
            if part in self.synonyms:
                syn = self.synonyms[part]
                if syn in self.topics:

                    #if 'fortios' in filename:
                    #    import epdb; epdb.st()

                    return syn
        for part in fparts[::-1]:
            if part in self.synonyms:
                syn = self.synonyms[part]
                for topic in self.topics:
                    if not '.' in topic:
                        continue
                    if topic.startswith(syn + '.') or topic.endswith('.' + syn):

                        #if 'fortios' in filename:
                        #    import epdb; epdb.st()

                        return topic

        # is this a _ delimited name?
        if '_' in bn:
            _bn = bn.split('_')[0]
            for pf in self.pluginfiles:
                if not pf[2]:
                    continue
                xbn = os.path.basename(pf[-1]).replace('.py', '').replace('.ini', '').replace('.yml', '')
                if '_' in xbn:
                    xbn = xbn.split('_')[0]
                if xbn == _bn:
                    logger.debug('A(3). %s --> %s' % (filename, pf[2]))
                    return pf[2]

        # fill in topics via synonyms
        for idx,x in enumerate(self.pluginfiles):

            if not x[2]:
                continue

            for a,b in self.synonyms.items():
                if a in bn:

                    if not '.' in x[2] and b == x[2]:
                        logger.debug('B. %s --> %s' % (filename, x[2]))
                        return x[2]
                    elif x[2].endswith(bn):
                        logger.debug('C. %s --> %s' % (filename, x[2]))
                        return x[2]
                    elif os.path.dirname(x[-1]).endswith(b):
                        logger.debug('D. %s --> %s' % (filename, x[2]))
                        return x[2]

                    xdn = x[-1].replace(self.checkout_dir + '/', '')
                    if b in xdn:
                        logger.debug('E. %s --> %s' % (filename, x[2]))
                        return x[2]
                    #import epdb; epdb.st()

        return None


    def get_plugins(self):

        # enumerate the modules
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules')
        for dirName, subdirList, fileList in os.walk(root):

            for fn in set(fileList) - {'__init__.py', 'loader.py'}:
                fp = os.path.join(dirName, fn)

                topic = os.path.relpath(fp, root)
                topic = os.path.dirname(topic)
                topic = topic.replace('/', '.')

                self.pluginfiles.append(['modules', fn, topic, fp])

        # make a list of unique topics
        self.topics = sorted(set(x[2] for x in self.pluginfiles))

        # enumerate the module utils
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'module_utils')
        for dirName, subdirList, fileList in os.walk(root):

            for fn in set(fileList) - {'__init__.py', 'loader.py'}:
                fp = os.path.join(dirName, fn)
                topic = self._guess_topic(fp)
                self.pluginfiles.append(['module_utils', fn, topic, fp])

        # enumerate all the other plugins
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'plugins')
        for dirName, subdirList, fileList in os.walk(root):

            for fn in set(fileList) - {'__init__.py', 'loader.py'}:
                ptype = os.path.basename(dirName)
                fp = os.path.join(dirName, fn)
                self.pluginfiles.append([ptype, fn, None, fp])

        # let's get rid of contrib too
        root = os.path.join(self.checkout_dir, 'contrib', 'inventory')
        for dirName, subdirList, fileList in os.walk(root):
            ptype = 'scripts'
            for fn in fileList:
                fp = os.path.join(dirName, fn)
                bn = os.path.basename(fn).replace('.py', '').replace('.ini', '')
                topic = self._guess_topic(fp)
                self.pluginfiles.append([ptype, fn, topic, fp])

        # guess the rest 
        for idx,x in enumerate(self.pluginfiles):
            if x[2]:
                continue
            self.pluginfiles[idx][2] = self._guess_topic(x[-1])

        # find which modules use orphaned doc fragments
        for idx,x in enumerate(self.pluginfiles):
            if x[2]:
                continue
            if x[0] != 'doc_fragments':
                continue
            df = os.path.basename(x[-1])
            df = df.replace('.py', '')
            cmd = 'find %s -type f | xargs fgrep -iH %s' % (os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules'), df)
            with contextlib.suppress(subprocess.CalledProcessError):
                filenames = subprocess.check_output(cmd, shell=True, text=True).split('\n')
                filenames = [x.split(':')[0] for x in filenames if x]
                filenames = sorted(set(filenames))
                dirnames = [os.path.dirname(x) for x in filenames]
                dirnames = [x.replace(os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules') + '/', '') for x in dirnames]
                dirnames = sorted(set(dirnames))
                logger.info('%s dirs' % len(dirnames))

        self.orphaned = [x for x in self.pluginfiles if not x[-2]]

    def make_spec(self):
        topics = self.topics[:]
        for idx,x in enumerate(topics):
            if not '.' in x:
                topics[idx] = x + '.misc'
        self.collections['_core'] = {}
        self.collections['orphaned.misc'] = {}
        for topic in topics:
            self.collections[topic] = {}

        for idx,x in enumerate(self.pluginfiles):
            topic = x[2]
            if topic is None:
                #self.collections['_orphaned'].append(x[-1])
                #continue
                topic = 'orphaned'
            ptype = x[0]
            if not '.' in topic:
                topic = topic + '.misc'
            if topic not in self.collections:
                self.collections[topic] = {}
            if ptype not in self.collections[topic]:
                self.collections[topic][ptype] = []

            spec_path = x[3].replace(os.path.join(self.checkout_dir, 'lib', 'ansible'), '')
            spec_path = spec_path.lstrip('/')
            if spec_path.startswith('modules/'):
                spec_path = spec_path.replace('modules/', '', 1)
            elif spec_path.startswith('module_utils/'):
                spec_path = spec_path.replace('module_utils/', '', 1)
            elif spec_path.startswith('plugins/'):
                spec_path = spec_path.replace('plugins/', '', 1)
                pos = spec_path.index('/') + 1
                spec_path = spec_path[pos:]
            elif 'contrib/inventory' in spec_path:
                spec_path = os.path.join('contrib', 'inventory', os.path.basename(spec_path))
            else:
                import epdb; epdb.st()
            
            #self.collections[topic][ptype].append(x[1])
            self.collections[topic][ptype].append(spec_path)
            #import epdb; epdb.st()

        #self.collections['_orphaned'] = sorted(self.collections['_orphaned'])
        for k,v in self.collections.items():
            #if k == '_orphaned':
            #    continue
            for ptype, pfiles in v.items():
                self.collections[k][ptype] = sorted(pfiles)

        if os.path.exists('status_quo'):
            shutil.rmtree('status_quo')
        os.makedirs('status_quo')

        namespaces = {}
        for k,v in self.collections.items():
            if '.' not in k:
                continue
            namespace = k.split('.')[0]
            name = k.split('.')[1]
            if namespace not in namespaces:
                namespaces[namespace] = {}
            namespaces[namespace][name] = copy.deepcopy(v)

        for namespace,names in namespaces.items():
            fn = os.path.join('status_quo', namespace + '.yml')
            with open(fn, 'w') as f:
                ruamel.yaml.dump(names, f, Dumper=ruamel.yaml.RoundTripDumper)

            # comment out the contrib scripts until migrate.py can support them
            has_scripts = [x.get('scripts') for x in names.values()]
            has_scripts = [x for x in has_scripts if x]
            if has_scripts:
                logger.info('commenting out scripts in %s' % namespace)
                with open(fn, 'r') as f:
                    fdata = f.read()
                fdata = fdata.replace('scripts:', '#scripts:')
                fdata = fdata.replace('- contrib/inventory', '#- contrib/inventory')
                os.remove(fn)
                with open(fn, 'w') as f:
                    f.write(fdata)


if __name__ == "__main__":
    StatusQuo.run()
