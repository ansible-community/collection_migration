#!/usr/bin/env python3

#################################################################
# update_nwo.py - recreates scenarios/nwo with current plugins
# 
# instructions:
#   1) virtualenv --python=$(which python3) venv
#   2) source venv/bin/activate
#   3) pip install -r requirements_nwo.txt
#   4) ./update_nwo.py
#   5) rm -rf scenarios/nwo
#   6) mv scenarios/nwo.new scenarios/nwo
#
#################################################################


import argparse
import contextlib
import copy
import csv
import itertools
import glob
import json
import os
import pickle
import re
import shutil
import subprocess

import yaml
from logzero import logger
import ruamel.yaml
import git as pygit
from sh import git
from sh import find

from pprint import pprint

from ansibullbot.utils.component_tools import AnsibleComponentMatcher
from ansibullbot.utils.git_tools import GitRepoWrapper


class UpdateNWO:

    SCENARIO = 'nwo'
    DUMPING_GROUND = ('community', 'general')

    collections = None
    plugins = None
    pluginfiles = None
    orphaned = None
    topics = None

    def __init__(self):

        self.scenario_output_dir = os.path.join('scenarios', self.SCENARIO + '.new')

        self.component_matcher = None
        self.galaxyindexer = None
        self.cachedir = '.cache'
        self.pluginfiles = []
        self.collections = {}
        self.url = 'https://github.com/ansible/ansible'
        self.checkouts_dir = '.cache/checkouts'
        self.checkout_dir = os.path.join(self.checkouts_dir, 'ansible')
        self.community_general_topics = None

        self.scenario_cache = {}

        self.rules = []

    def run(self, usecache=False, galaxy_indexer=None, base_scenario_file=None):

        if os.path.exists(self.scenario_output_dir):
            shutil.rmtree(self.scenario_output_dir)
        os.makedirs(self.scenario_output_dir)

        if galaxy_indexer:
            self.galaxy_indexer = galaxy_indexer

        self.map_existing_files_to_rules()
        self.manage_checkout()

        # ansibot magic
        gitrepo = GitRepoWrapper(
            cachedir=self.cachedir,
            repo=self.url
        )
        self.component_matcher = AnsibleComponentMatcher(
            gitrepo=gitrepo,
            email_cache={}
        )

        self.get_plugins()
        self.map_plugins_to_collections()

        self.make_spec()
        self.make_compiled_csv()

    def map_existing_files_to_rules(self):

        ''' Make a set of matching rules based on current scenario files '''

        sfiles = glob.glob('scenarios/%s/*.yml' % self.SCENARIO) 
        sfiles = sorted(set(sfiles))

        for sfile in sfiles:
            with open(sfile, 'r') as f:
                ydata = yaml.load(f.read())
            namespace = os.path.basename(sfile).replace('.yml', '')
            self.scenario_cache[namespace] = copy.deepcopy(ydata)
            for name,plugins in ydata.items():

                if namespace == 'community' and name == 'general':
                    continue

                for ptype,pfiles in plugins.items():
                    for pfile in pfiles:
                        self.rules.append({
                            'plugin_type': ptype,
                            'matcher': pfile,
                            'namespace': namespace,
                            'name': name,
                            'source': sfile
                        })

    def manage_checkout(self):

        ''' Could probably be replaced with pygit '''

        logger.info('manage ansible checkout for NWO updates')

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

    def _guess_collection(self, plugin_type=None, plugin_basename=None, plugin_relpath=None, plugin_filepath=None):

        ''' use rules to match a plugin file to a namespace.name '''

        logger.debug(plugin_filepath)

        ppaths = plugin_relpath.split('/')
        matched_rules = []
        for rule in self.rules:

            # no need to process different plugin type
            if rule['plugin_type'] != plugin_type:
                continue

            # simplest match
            if rule['matcher'] == plugin_relpath:
                logger.debug('1')
                matched_rules.append(rule)
                continue

            # globs are FUN ...
            if '*' in rule['matcher']:

                mpaths = rule['matcher'].split('/')
                zipped = list(itertools.zip_longest(mpaths, ppaths))
                bad = [False for x in zipped if x[0] != x[1] and x[0] != '*']

                if not bad:
                    matched_rules.append(rule)
                    continue

            # globs are MORE FUN ...
            if '*' in rule['matcher']:
                if re.match(rule['matcher'], plugin_relpath):
                    matched_rules.append(rule)
                    continue


        if len(matched_rules) == 1:
            return (matched_rules[0]['namespace'], matched_rules[0]['name'], matched_rules[0])        
        elif len(matched_rules) > 1:
            # use most specific match?
            for mr in matched_rules:
                mpaths = mr['matcher'].split('/')
                if len(mpaths) == len(ppaths):
                    return (mr['namespace'], mr['name'], mr)        

        return (
            self.DUMPING_GROUND[0],
            self.DUMPING_GROUND[1],
            {
                'plugin_type': plugin_type,
                'matcher': 'unclaimed!',
                'namespace': self.DUMPING_GROUND[0],
                'name': self.DUMPING_GROUND[1]
            }
        )

    def get_plugins(self):

        ''' Find all plugins in the cached checkout and make a list '''

        # enumerate the modules
        logger.info('iterating through modules')
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules')
        for dirName, subdirList, fileList in os.walk(root):

            for fn in set(fileList) - {'__init__.py', 'loader.py'}:
                fp = os.path.join(dirName, fn)
                topic = None
                self.pluginfiles.append(['modules', fn, topic, fp])

        # enumerate the module utils
        logger.info('iterating through module utils')
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'module_utils')
        for dirName, subdirList, fileList in os.walk(root):

            for fn in set(fileList) - {'__init__.py', 'loader.py'}:
                fp = os.path.join(dirName, fn)
                #topic = self._guess_topic(fp)
                topic = None
                self.pluginfiles.append(['module_utils', fn, topic, fp])

        # enumerate all the other plugins
        logger.info('examining other plugins')
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'plugins')
        for dirName, subdirList, fileList in os.walk(root):

            for fn in set(fileList) - {'__init__.py', 'loader.py'}:
                ptype = os.path.basename(dirName)
                fp = os.path.join(dirName, fn)
                self.pluginfiles.append([ptype, fn, None, fp])

        '''
        # let's get rid of contrib too
        logger.info('looking at contrib scripts')
        root = os.path.join(self.checkout_dir, 'contrib', 'inventory')
        for dirName, subdirList, fileList in os.walk(root):
            ptype = 'scripts'
            for fn in fileList:
                fp = os.path.join(dirName, fn)
                bn = os.path.basename(fn).replace('.py', '').replace('.ini', '')
                #topic = self._guess_topic(fp)
                topic = None
                self.pluginfiles.append([ptype, fn, topic, fp])
        '''


    def map_plugins_to_collections(self):

        ''' associate each plugin to a collection '''

        self.community_general_topics = set()
        self.topics = set()

        # enumerate the modules
        logger.info('iterating through modules')
        root = os.path.join(self.checkout_dir, 'lib', 'ansible', 'modules')
        for dirName, subdirList, fileList in os.walk(root):

            for fn in set(fileList) - {'__init__.py', 'loader.py'}:
                fp = os.path.join(dirName, fn)
                topic = os.path.relpath(fp, root)
                topic = os.path.dirname(topic)
                topic = topic.replace('/', '.')
                self.topics.add(topic)
                self.community_general_topics.add(topic)

        logger.info('matching %s files' % len(self.pluginfiles))
        for idp,plugin in enumerate(self.pluginfiles):
            filepath = plugin[3].replace(self.checkout_dir + '/', '')
            if '/plugins/' in plugin[3]:
                relpath = plugin[3].replace(self.checkout_dir + '/lib/ansible/plugins/%s/' % plugin[0], '')
            else:
                relpath = plugin[3].replace(self.checkout_dir + '/lib/ansible/%s/' % plugin[0], '')

            this_namespace, this_name, this_rule = self._guess_collection(
                plugin_type=plugin[0],
                plugin_basename=plugin[1],
                plugin_filepath=filepath,
                plugin_relpath=relpath,
            )
            self.pluginfiles[idp][2] = (this_namespace, this_name)
            self.pluginfiles[idp].append(this_rule)
            logger.debug('%s.%s:%s:%s > %s' % (this_namespace, this_name, this_rule['plugin_type'], this_rule['matcher'], plugin[3]))
        logger.info('files matching done')
        self.pluginfiles = sorted(self.pluginfiles, key=lambda x: x[3])

    def make_compiled_csv(self):
        
        ''' Make the human readable aggregated spreadsheet '''

        fn = os.path.join(self.scenario_output_dir, 'compiled.csv')        
        logger.info('compiling %s' % fn)
        with open(fn, 'w') as csvfile:
            spamwriter = csv.writer(csvfile)
            spamwriter.writerow([
                'filename',
                'fqn',
                'namespace',
                'name',
                'current_support_level',
                'new_support_level',
                'scenario_file',
                'scenario_plugin_type',
                'matched_line']
            )

            for pf in self.pluginfiles:

                ns = pf[2][0]
                name = pf[2][1]
                fqn = '%s.%s' % (ns, name)
                if ns == 'ansible' and name == '_core':
                    fqn = 'base'

                relpath = pf[3].replace(self.checkout_dir+'/', '')
                meta = self.component_matcher.get_meta_for_file(relpath)

                new_support = 'community'
                if pf[2][0] == 'ansible' and pf[2][1] == '_core':
                    new_support = 'core'
                    name = 'base'

                row = [
                    relpath,
                    fqn,
                    ns,
                    name,
                    meta['support'],
                    new_support,
                    'scenarios/nwo/%s.yml' % ns,
                    pf[4]['plugin_type'],
                    pf[4]['matcher']
                ]
                if ns == 'community' and name == 'general':
                    row[6] = 'unclaimed!'
                spamwriter.writerow(row)

    def _make_relpath(self, filename, plugintype):
        # .cache/checkouts/ansible/lib/ansible/module_utils/foo/bar/acme.py
        # foo/bar/acme.py
        pindex = filename.index('/'+plugintype)
        relpath = filename[pindex+len(plugintype)+2:]
        return relpath

    def make_spec(self):

        # make specfile ready dicts for each collection
        for idx,x in enumerate(self.pluginfiles):
            ns = x[-1]['namespace']
            name = x[-1]['name']
            ckey = (ns, name)
            ptype = x[0]
            matcher = x[-1]['matcher']            

            # use the relative path for unmatched community files
            if 'unclaimed' in matcher:
                matcher =  self._make_relpath(x[3], ptype)

            if ckey not in self.collections:
                self.collections[ckey] = {}

            if ptype not in self.collections[ckey]:
                self.collections[ckey][ptype] = []
            if matcher not in self.collections[ckey][ptype]:
                self.collections[ckey][ptype].append(matcher)

        # sort the filepaths in each plugin type
        for k,v in self.collections.items():
            for ptype, pfiles in v.items():
                self.collections[k][ptype] = sorted(pfiles)

        # squash all collections into their namespaces
        namespaces = {}
        for ckey, collection in self.collections.items():
            if ckey[0] not in namespaces:
                namespaces[ckey[0]] = {}
            namespaces[ckey[0]][ckey[1]] = copy.deepcopy(collection)

        # write each namespace as a separate file
        for namespace,collections in namespaces.items():
            fn = os.path.join(self.scenario_output_dir, namespace + '.yml')
            logger.info('write %s' % fn)
            with open(fn, 'w') as f:

                if namespace != 'community':
                    ruamel.yaml.dump(self.scenario_cache[namespace], f, Dumper=ruamel.yaml.RoundTripDumper)
                else:
                    this_data = copy.deepcopy(self.scenario_cache['community'])
                    this_data['general'] = collections['general']
                    ruamel.yaml.dump(this_data, f, Dumper=ruamel.yaml.RoundTripDumper)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--usecache', action='store_true')
    args = parser.parse_args()

    nwo = UpdateNWO()
    nwo.run(usecache=args.usecache)
