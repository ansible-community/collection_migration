#!/usr/bin/env python

# also dynamically imports ansible in code

import argparse
import configparser
import contextlib
import glob
import itertools
import logging
import os
import re
import shutil
import subprocess
import sys
import textwrap
import yaml

from collections import defaultdict
from collections.abc import Mapping
from importlib import import_module
from string import Template

from ansible.parsing.yaml.dumper import AnsibleDumper
from ansible.parsing.yaml.loader import AnsibleLoader
from ansible.vars.reserved import is_reserved_name

import logzero
from logzero import logger

from baron.parser import ParsingError
import redbaron

from gh import GitHubOrgClient


# https://github.com/ansible/ansible/blob/100fe52860f45238ee8ca9e3019d1129ad043c68/hacking/fix_test_syntax.py#L62
FILTER_RE = re.compile(r'((.+?)\s*([\w \.\'"]+)(\s*)\|(\s*)(\w+))')
TEST_RE = re.compile(r'((.+?)\s*([\w \.\'"]+)(\s*)is(\s*)(\w+))')

DEVEL_URL = 'https://github.com/ansible/ansible.git'
DEVEL_BRANCH = 'devel'
MIGRATED_DEVEL_URL = 'git@github.com:ansible/migratedcore.git'


VARDIR = os.environ.get('GRAVITY_VAR_DIR', '.cache')
COLLECTION_NAMESPACE = 'test_migrate_ns'
PLUGIN_EXCEPTION_PATHS = {'modules': 'lib/ansible/modules', 'module_utils': 'lib/ansible/module_utils', 'lookups': 'lib/ansible/plugins/lookup'}


COLLECTION_SKIP_REWRITE = ('_core',)


RAW_STR_TMPL = "r'''{str_val}'''"
STR_TMPL = "'''{str_val}'''"


os.makedirs(VARDIR, exist_ok=True)
logzero.logfile(os.path.join(VARDIR, 'errors.log'), loglevel=logging.WARNING)


core = {}
manual_check = defaultdict(list)


def add_core(ptype, name):

    global core
    if ptype not in core:
        core[ptype] = set()

    core[ptype].add(name)


def add_manual_check(key, value, filename):
    global manual_check
    manual_check[filename].append((key, value))


def checkout_repo(vardir=VARDIR, refresh=False):
    releases_dir = os.path.join(vardir, 'releases')
    devel_path = os.path.join(releases_dir, f'{DEVEL_BRANCH}.git')

    if refresh and os.path.exists(devel_path):
        subprocess.check_call(('git', 'checkout', DEVEL_BRANCH), cwd=devel_path)
        subprocess.check_call(('git', 'pull', '--rebase'), cwd=devel_path)

    if not os.path.exists(releases_dir):
        os.makedirs(releases_dir)

    if not os.path.exists(devel_path):
        subprocess.check_call(('git', 'clone', DEVEL_URL, f'{DEVEL_BRANCH}.git'), cwd=releases_dir)


# ===== FILE utils =====
REMOVE = set()


def remove(path):
    global REMOVE
    REMOVE.add(path)


def actually_remove(namespace, collection, vardir=VARDIR):
    global REMOVE
    releases_dir = os.path.join(vardir, 'releases')
    devel_path = os.path.join(releases_dir, f'{DEVEL_BRANCH}.git')

    for path in REMOVE:
        actual_devel_path = os.path.relpath(path, devel_path)
        subprocess.check_call(('git', 'rm', actual_devel_path), cwd=devel_path)

    subprocess.check_call(('git', 'commit', '-m', 'Migrated to %s.%s' % (namespace, collection)), cwd=devel_path)


def read_yaml_file(path):
    with open(path, 'rb') as yaml_file:
        return yaml.safe_load(yaml_file)


def read_ansible_yaml_file(path):
    with open(path, 'rb') as yaml_file:
        return AnsibleLoader(yaml_file.read(), file_name=path).get_single_data()


def write_yaml_into_file_as_is(path, data):
    yaml_text = yaml.dump(data, allow_unicode=True, default_flow_style=False, sort_keys=False)
    write_text_into_file(path, yaml_text)


def write_ansible_yaml_into_file_as_is(path, data):
    yaml_text = yaml.dump(data, Dumper=AnsibleDumper, allow_unicode=True, default_flow_style=False, sort_keys=False)
    write_text_into_file(path, yaml_text)


def read_text_from_file(path):
    with open(path, 'r') as f:
        return f.read()


def write_text_into_file(path, text):
    with open(path, 'w') as f:
        return f.write(text)


# ===== SPEC utils =====
def load_spec_file(spec_file):

    spec = read_yaml_file(spec_file)  # TODO: capture yamlerror?

    if not isinstance(spec, Mapping):
        sys.exit("Invalid format for spec file, expected a dictionary and got %s" % type(spec))
    elif not spec:
        sys.exit("Cannot use spec file, ended up with empty spec")

    return spec


def resolve_spec(spec, checkoutdir):

    # TODO: add negation? entry: x/* \n entry: !x/base.py
    for ns in spec.keys():
        for coll in spec[ns].keys():
            for ptype in spec[ns][coll].keys():
                plugin_base = os.path.join(checkoutdir, PLUGIN_EXCEPTION_PATHS.get(ptype, os.path.join('lib', 'ansible', 'plugins', ptype)))
                replace_base = '%s/' % plugin_base
                for entry in spec[ns][coll][ptype]:
                    if r'*' in entry or r'?' in entry:
                        files = glob.glob(os.path.join(plugin_base, entry))
                        for fname in files:
                            if ptype != 'module_utils' and fname.endswith('__init__.py') or not os.path.isfile(fname):
                                continue
                            fname = fname.replace(replace_base, '')
                            spec[ns][coll][ptype].insert(0, fname)

                        # clean out glob entry
                        spec[ns][coll][ptype].remove(entry)


# ===== GET_PLUGINS utils =====
def get_plugin_collection(plugin_name, plugin_type, spec):
    for ns in spec.keys():
        for collection in spec[ns].keys():
            if spec[ns][collection]: # avoid empty collections
                plugins = spec[ns][collection].get(plugin_type, [])
                if plugin_name + '.py' in plugins:
                    return ns, collection

    # keep info
    plugin_name = plugin_name.replace('/', '.')
    logger.debug('Assuming "%s.%s " stays in core' % (plugin_type, plugin_name))
    add_core(plugin_type, plugin_name.replace('/', '.'))

    raise LookupError('Could not find "%s" named "%s" in any collection in the spec' % (plugin_type, plugin_name))


def get_plugins_from_collection(ns, collection, plugin_type, spec):
    assert ns in spec
    assert collection in spec[ns]
    return [plugin.rsplit('/')[-1][:-3] for plugin in spec[ns][collection].get(plugin_type, [])]


def get_plugin_fqcn(namespace, collection, plugin_name):
    return '%s.%s.%s' % (namespace, collection, plugin_name)


def get_rewritable_collections(namespace, spec):
    return (collection for collection in spec[namespace].keys() if collection not in COLLECTION_SKIP_REWRITE)


# ===== REWRITE FUNCTIONS =====
def rewrite_class_property(mod_fst, collection, namespace, filename):
    rewrite_map = {
        'BecomeModule': 'name',
        'CallbackModule': 'CALLBACK_NAME',
        'Connection': 'transport',
        'InventoryModule': 'NAME',
    }

    for class_name, property_name in rewrite_map.items():
        try:
            val = (
                mod_fst.
                find('class', name=class_name).
                find('name', value=property_name).parent.value
            )
        except AttributeError:
            continue

        try:
            val.value = "'%s'" % get_plugin_fqcn(namespace,
                                                 collection,
                                                 val.to_python())
        except ValueError as e:
            # so this might be something like:
            # transport = CONNECTION_TRANSPORT
            add_manual_check(property_name, val.value, filename)


def rewrite_unit_tests_patch(mod_fst, collection, spec, namespace, args, filename):
    # FIXME duplicate code from imports rewrite
    plugins_path = ('ansible_collections', namespace, collection, 'plugins')
    tests_path = ('ansible_collections', namespace, collection, 'tests')
    unit_tests_path = tests_path + ('unit', )
    import_map = {
        ('ansible', 'modules'): plugins_path + ('modules', ),
        ('ansible', 'module_utils'): plugins_path + ('module_utils', ),
        ('ansible', 'plugins'): plugins_path,
        ('units', ): unit_tests_path,
    }

    deps = []
    for patch in mod_fst('decorator', lambda x: x.dumps().startswith('@patch(')) + mod_fst('assignment', lambda x: 'patch(' in x.dumps())+ mod_fst('with', lambda x: 'patch(' in x.dumps()):
        try:
            value = patch.call.value
        except AttributeError:
            continue

        for el in value:
            try:
                val = el.value.to_python().split('.')
            except IndentationError as e:
                logger.error(e)
                add_manual_check(patch.dumps(), str(e), filename)
                continue
            except (AttributeError, TypeError, ValueError) as e:
                # might be something else, like 'side_effect=...'
                continue

            for old, new in import_map.items():
                token_length = len(old)
                if tuple(val[:token_length]) != old:
                    continue

                if val[1] in ('modules', 'module_utils'):
                    # FIXME account for preserve module subdirs
                    plugin_type = val[1]

                    # patch('ansible.modules.storage.netapp.na_ontap_nvme.NetAppONTAPNVMe.create_nvme')
                    # look for module name
                    for i in (len(val), -1, -2):
                        plugin_name = '/'.join(val[2:i])
                        try:
                            found_ns, found_coll = get_plugin_collection(plugin_name, plugin_type, spec)
                            break
                        except LookupError:
                            continue
                    else:
                        continue
                elif val[1] == 'plugins':
                    # patch('ansible.plugins.lookup.manifold.open_url')
                    plugin_type = val[2]
                    plugin_name = val[3]

                    try:
                        found_ns, found_coll = get_plugin_collection(plugin_name, plugin_type, spec)
                    except LookupError:
                        continue
                else:
                    continue

                if found_coll in COLLECTION_SKIP_REWRITE:
                    continue

                if args.fail_on_core_rewrite:
                    raise RuntimeError('Rewriting to %s' % '.'.join(val))

                val[:token_length] = new
                if (found_ns, found_coll) != (namespace, collection):
                    val[1] = found_ns
                    val[2] = found_coll
                    deps.append((found_ns, found_coll))

                el.value = "'%s'" % '.'.join(val)

    return deps


def rewrite_doc_fragments(mod_fst, collection, spec, namespace, args):
    try:
        doc_val = (
            mod_fst.
            find_all('assignment').
            find('name', value='DOCUMENTATION').
            parent.
            value
        )
    except AttributeError:
        raise LookupError('No DOCUMENTATION found')

    docs_parsed = yaml.safe_load(doc_val.to_python().strip('\n'))

    fragments = docs_parsed.get('extends_documentation_fragment', [])
    if not fragments:
        return []

    if not isinstance(fragments, list):
        fragments = [fragments]

    deps = []
    new_fragments = []
    for fragment in fragments:
        # some doc_fragments use subsections (e.g. vmware.vcenter_documentation)
        fragment_name, _dot, _rest = fragment.partition('.')
        try:
            fragment_namespace, fragment_collection = get_plugin_collection(fragment_name, 'doc_fragments', spec)
        except LookupError:
            # plugin not in spec, assuming it stays in core and leaving as is
            new_fragments.append(fragment)
            continue

        if fragment_collection in COLLECTION_SKIP_REWRITE:
            # skip rewrite
            new_fragments.append(fragment)
            continue

        if fragment_collection.startswith('_'):
            fragment_collection = fragment_collection[1:]

        new_fragment = get_plugin_fqcn(fragment_namespace, fragment_collection, fragment)
        if args.fail_on_core_rewrite:
            raise RuntimeError('Rewriting to %s' % new_fragment)

        new_fragments.append(new_fragment)

        if (namespace, collection) != (fragment_namespace, fragment_collection):
            deps.append((fragment_namespace, fragment_collection))

    docs_parsed['extends_documentation_fragment'] = new_fragments

    doc_str_tmpl = RAW_STR_TMPL if doc_val.type == 'raw_string' else STR_TMPL
    # `doc_val` holds a baron representation of the string node
    # of type 'string' or 'raw_string'. Updating its `.value`
    # via assigning the new one replaces the node in FST.
    # Also, in order to generate a string or raw-string literal,
    # we need to wrap it with a corresponding pair of quotes.
    # If we don't do this, we'd generate the following Python code
    # ```
    # DOCUMENTATION = some string value
    # ```
    # instead of the correct
    # ```
    # DOCUMENTATION = r'''some string value'''
    # ```
    # or
    # ```
    # DOCUMENTATION = '''some string value'''
    # ```
    doc_val.value = doc_str_tmpl.format(
        str_val=yaml.dump(docs_parsed),
    )

    return deps


def rewrite_imports(mod_fst, collection, spec, namespace, args):
    """Rewrite imports map."""
    plugins_path = ('ansible_collections', namespace, collection, 'plugins')
    tests_path = ('ansible_collections', namespace, collection, 'tests')
    unit_tests_path = tests_path + ('unit', )
    import_map = {
        ('ansible', 'modules'): plugins_path + ('modules', ),
        ('ansible', 'module_utils'): plugins_path + ('module_utils', ),
        ('ansible', 'plugins'): plugins_path,
        ('units', ): unit_tests_path,
    }

    return rewrite_imports_in_fst(mod_fst, import_map, collection, spec, namespace, args)


def match_import_src(imp_src, import_map):
    """Find a replacement map entry matching the current import."""
    try:
        imp_src_tuple = tuple(t.value for t in imp_src)
    except AttributeError as e:
        # AttributeError("EllipsisNode instance has no attribute 'value' and 'value' is not a valid identifier of another node")
        # lib/ansible/modules/system/setup.py:
        # from ...module_utils.basic import AnsibleModule
        logger.exception(e)
        raise LookupError

    for old_imp, new_imp in import_map.items():
        token_length = len(old_imp)
        if imp_src_tuple[:token_length] != old_imp:
            continue
        return token_length, new_imp

    raise LookupError(f"Couldn't find a replacement for {imp_src!s}")


def rewrite_imports_in_fst(mod_fst, import_map, collection, spec, namespace, args):
    """Replace imports in the python module FST."""
    deps = []
    for imp in mod_fst.find_all(('import', 'from_import')):
        imp_src = imp.value
        if imp.type == 'import':
            imp_src = imp_src[0].value

        try:
            token_length, exchange = match_import_src(imp_src, import_map)
        except LookupError:
            continue  # no matching imports

        if not imp.find('name', value='g:*module_utils*') and imp.find_all('name_as_name', value='g:*Base*'):
            # from ansible.plugins.lookup import LookupBase
            # NOT 'from ansible.module_utils.azure_rm_common import AzureRMModuleBase'
            continue  # Skip imports of Base classes

        if imp.find('name_as_name', value='g:*loader*'):
            continue

        if imp.find('name_as_name', value=('AnsiblePlugin', 'PluginLoader')):
            # from ansible.plugins import AnsiblePlugin
            # from ansible.plugins.loader import PluginLoader
            continue

        if imp_src[0].value == 'units':
            imp_src[:token_length] = exchange  # replace the import
            continue
        elif imp_src[1].value == 'module_utils':
            plugin_type = 'module_utils'
            plugin_name = '/'.join(t.value for t in imp_src[token_length:])
            if not plugin_name:
                # 'from ansible.module_utils import distro'
                plugin_name = imp.targets[0].value
                # NOTE multiple targets? - git grep says there is not such case now
                # NOTE 'from ansible.module_utils import $module as $alias'? - git grep says there is not such case now
        elif imp_src[1].value == 'plugins':
            try:
                plugin_type = imp_src[2].value
                plugin_name = imp_src[3].value
            except IndexError:
                if len(getattr(imp, 'targets', [])) == 1:
                    # from ansible.plugins.connection import winrm
                    plugin_name = imp.targets[0].value
                else:
                    logger.error('Could not get plugin type or name from ' + str(imp) + '. Is this expected?')
                    continue
        elif imp_src[1].value == 'modules':
            # in unit tests
            plugin_type = 'modules'
            try:
                # from ansible.modules.network.nxos import nxos_bgp
                plugin_name = '/'.join([t.value for t in imp_src[token_length:]] + [imp.targets[0].value])
            except AttributeError:
                # import ansible.modules.cloud.amazon.aws_api_gateway as agw
                plugin_name = '/'.join(t.value for t in imp_src[token_length:])
        else:
            raise Exception('BUG: Could not process import: ' + str(imp))

        try:
            plugin_namespace, plugin_collection = get_plugin_collection(plugin_name, plugin_type, spec)
        except LookupError as e:
            if plugin_type != 'modules':
                # plugin not in spec, assuming it stays in core and skipping
                continue

            # from ansible.modules.cloud.amazon.aws_netapp_cvs_FileSystems import AwsCvsNetappFileSystem as fileSystem_module
            # in this case aws_netapp_cvs_FileSystems is the module, not AwsCvsNetappFileSystem
            try:
                plugin_name = '/'.join(plugin_name.split('/')[:-1])
                plugin_namespace, plugin_collection = get_plugin_collection(plugin_name, plugin_type, spec)
            except LookupError as e:
                # plugin not in spec, assuming it stays in core and skipping
                continue

        if plugin_collection in COLLECTION_SKIP_REWRITE:
            # skip rewrite
            continue

        if args.fail_on_core_rewrite:
            raise RuntimeError('Rewriting to %s.%s.%s' % (plugin_namespace, plugin_collection, plugin_name))

        if plugin_collection.startswith('_'):
            plugin_collection = plugin_collection[1:]

        imp_src[:token_length] = exchange  # replace the import
        if (plugin_namespace, plugin_collection) != (namespace, collection):
            imp_src[1] = plugin_namespace
            imp_src[2] = plugin_collection
            deps.append((plugin_namespace, plugin_collection))

    return deps


def read_module_txt_n_fst(path):
    """Parse module source code in form of Full Syntax Tree."""
    mod_src_text = read_text_from_file(path)
    try:
        return mod_src_text, redbaron.RedBaron(mod_src_text)
    except ParsingError:
        logger.exception('failed parsing on %s', mod_src_text)
        raise


def inject_init_into_tree(target_dir):
    for initpath in (
            os.path.join(dp, '__init__.py')
            for dp, dn, fn in os.walk(target_dir)
            if '__init__.py' not in fn
            # and any(f.endwith('.py') for f in fn)
    ):
        write_text_into_file(initpath, '')


def inject_gitignore_into_tests(collection_dir):
    """Generate a ``.gitignore`` file for the collection tests dir."""
    os.makedirs(os.path.join(collection_dir, 'tests'), exist_ok=True)
    write_text_into_file(
        os.path.join(collection_dir, 'tests', '.gitignore'),
        textwrap.dedent('''
        output/
        ''').lstrip('\n'),
    )


def generate_converted_ignore_contents(original_ignore_contents, file_map):
    """Emit lines for the converted sanity test ignore file."""
    for line in original_ignore_contents.splitlines():
        file_path, sep, ignored_rules = line.partition(' ')
        with contextlib.suppress(KeyError):
            yield sep.join((file_map[file_path], ignored_rules))


def inject_ignore_into_sanity_tests(
        checkout_path, collection_dir, migrated_files_map,
):
    """Inject sanity test ignore lists into collection sanity tests."""
    coll_sanity_tests_dir = os.path.join(collection_dir, 'tests', 'sanity')
    original_ignore_contents = read_text_from_file(
        os.path.join(checkout_path, 'test', 'sanity', 'ignore.txt'),
    )
    converted_ignore_contents = '\n'.join(
        generate_converted_ignore_contents(
            original_ignore_contents,
            migrated_files_map,
        ),
    )

    if not converted_ignore_contents:
        return

    os.makedirs(coll_sanity_tests_dir, exist_ok=True)
    write_text_into_file(  # PyPI
        os.path.join(coll_sanity_tests_dir, 'ignore-2.9.txt'),
        converted_ignore_contents,
    )
    write_text_into_file(  # devel
        os.path.join(coll_sanity_tests_dir, 'ignore-2.10.txt'),
        converted_ignore_contents,
    )


def inject_requirements_into_unit_tests(checkout_path, collection_dir):
    """Inject unit tests dependencies into collection."""
    coll_unit_tests_dir = os.path.join(collection_dir, 'tests', 'unit')
    original_unit_tests_req_file = os.path.join(
        checkout_path, 'test', 'units', 'requirements.txt',
    )

    os.makedirs(coll_unit_tests_dir, exist_ok=True)
    shutil.copy(original_unit_tests_req_file, coll_unit_tests_dir)

    logger.info('Unit tests deps injected into collection')


def inject_requirements_into_integration_tests(checkout_path, collection_dir):
    """Inject integration tests Python dependencies into collection."""
    coll_integration_tests_dir = os.path.join(
        collection_dir, 'tests', 'integration',
    )
    original_integration_tests_req_file = os.path.join(
        checkout_path, 'test', 'integration', 'requirements.txt',
    )

    if os.path.exists(original_integration_tests_req_file):
        os.makedirs(coll_integration_tests_dir, exist_ok=True)
        shutil.copy( original_integration_tests_req_file, coll_integration_tests_dir)
        logger.info('Integration tests deps injected into collection')


def copy_unit_tests(checkout_path, collection_dir, plugin_type, plugin, spec):
    """Find all unit tests and related artifacts for the given plugin.

    Return the copy map.
    """
    type_subdir = (
        plugin_type
        if plugin_type in ('modules', 'module_utils')
        else os.path.join('plugins', plugin_type)
    )

    unit_tests_relative_root = os.path.join('test', 'units')

    collection_unit_tests_relative_root = os.path.join('tests', 'unit')

    # Narrow down the search area
    type_base_subdir = os.path.join(
        checkout_path, unit_tests_relative_root, type_subdir,
    )

    # Figure out what to copy and where
    copy_map = {}

    # Find all test modules with the same ending as the current plugin
    plugin_dir, plugin_mod = os.path.split(plugin)
    matching_test_modules = glob.glob(os.path.join(type_base_subdir, plugin_dir, f'*{plugin_mod}'))
    # Path(matching_test_modules[0]).relative_to(Path(checkout_path))
    # os.path.relpath(matching_test_modules[0], checkout_path)
    if not matching_test_modules:
        logger.info('No tests matching %s/%s found', plugin_type, plugin)
        return copy_map

    def traverse_dir(path, relative_to):
        rel_path = os.path.join(relative_to, path)
        if not os.path.isdir(rel_path):
            return {path}

        matching_files = itertools.chain(
            glob.iglob(
                os.path.join(rel_path, '.**'),
                recursive=True,
            ),
            glob.iglob(
                os.path.join(rel_path, '**'),
                recursive=True,
            ),
        )
        return set(
            os.path.relpath(p, relative_to)
            for p in matching_files
            if not os.path.isdir(p)
        )

    def replace_path_prefix(path):
        return os.path.join(
            collection_unit_tests_relative_root,
            os.path.relpath(path, unit_tests_relative_root),
        )

    # Inject unit test helper packages
    # TODO: figure out the bug with path maps
    compat_mock_helpers = (
        (
            src_f,
            replace_path_prefix(src_f),
        )
        for hd in {'compat', 'mock', 'modules/utils.py'}
        for src_f in traverse_dir(
            os.path.join(unit_tests_relative_root, hd),
            checkout_path,
        )
    )
    for src_f, dst_f in compat_mock_helpers:
        copy_map[src_f] = dst_f

    # Add test modules along with related artifacts
    for td, tm in (os.path.split(p) for p in matching_test_modules):
        relative_td = os.path.relpath(td, checkout_path)
        copy_map_relative_td_to = os.path.join(
            collection_unit_tests_relative_root,
            plugin_type, plugin_dir,
        )
        copy_map[os.path.join(relative_td, tm)] = os.path.join(copy_map_relative_td_to, tm)
        # Add subdirs that may contain related test artifacts/fixtures
        # Also add important modules like conftest or __init__
        for path in os.listdir(td):
            is_dir = os.path.isdir(os.path.join(td, path))
            if not is_dir and path.startswith('test_'):
                continue
            for src_f in traverse_dir(
                    os.path.join(relative_td, path),
                    checkout_path,
            ):
                copy_map[src_f] = replace_path_prefix(src_f)

    # Actually copy tests
    for src_f, dest_f in copy_map.items():
        dest = os.path.join(collection_dir, dest_f)
        # Ensure target dir exists
        os.makedirs(os.path.dirname(dest), exist_ok=True)

        src = os.path.join(checkout_path, src_f)
        shutil.copy(src, dest)

        if src == '.cache/releases/devel.git/test/units/modules/utils.py':
            # FIXME this appears to be bundled with each collection (above), so do not remove it, this should stay in core?
            continue

        remove(src)

    inject_requirements_into_unit_tests(checkout_path, collection_dir)

    logger.info('Unit tests copied for %s/%s', plugin_type, plugin)
    return copy_map


# ===== MAKE COLLECTIONS =====
def assemble_collections(spec, args, target_github_org):
    # NOTE releases_dir is already created by checkout_repo(), might want to move all that to something like ensure_dirs() ...
    releases_dir = os.path.join(args.vardir, 'releases')
    checkout_path = os.path.join(releases_dir, f'{DEVEL_BRANCH}.git')
    collections_base_dir = os.path.join(args.vardir, 'collections')
    meta_dir = os.path.join(args.vardir, 'meta')
    integration_test_dirs = []

    # expand globs so we deal with specific paths
    resolve_spec(spec, checkout_path)

    # ensure we always use a clean copy
    if args.refresh and os.path.exists(collections_base_dir):
        shutil.rmtree(collections_base_dir)

    # make initial YAML transformation to minimize the diff
    mark_moved_resources(checkout_path, 'N/A', 'init', set())

    seen = {}
    for namespace in spec.keys():
        for collection in spec[namespace].keys():
            import_deps = []
            docs_deps = []
            unit_deps = []

            if args.fail_on_core_rewrite:
                if collection != '_core':
                    continue
            else:
                if collection.startswith('_'):
                    # these are info only collections
                    continue

            migrated_to_collection = {}

            collection_dir = os.path.join(collections_base_dir, 'ansible_collections', namespace, collection)

            if args.refresh and os.path.exists(collection_dir):
                shutil.rmtree(collection_dir)

            if not os.path.exists(collection_dir):
                os.makedirs(collection_dir)

            # create the data for galaxy.yml
            galaxy_metadata = {
                'namespace': namespace,
                'name': collection,
                'version': '1.0.0',  # TODO: add to spec, args?
                'readme': None,
                'authors': None,
                'description': None,
                'license': None,
                'license_file': None,
                'tags': None,
                'dependencies': {},
                'repository': f'git@github.com:{target_github_org}/{namespace}.{collection}.git',
                'documentation': None,
                'homepage': None,
                'issues': None
            }

            for plugin_type in spec[namespace][collection].keys():

                # get right plugin path
                if plugin_type not in PLUGIN_EXCEPTION_PATHS:
                    src_plugin_base = os.path.join('lib', 'ansible', 'plugins', plugin_type)
                else:
                    src_plugin_base = PLUGIN_EXCEPTION_PATHS[plugin_type]

                # ensure destinations exist
                relative_dest_plugin_base = os.path.join('plugins', plugin_type)
                dest_plugin_base = os.path.join(collection_dir, relative_dest_plugin_base)
                if not os.path.exists(dest_plugin_base):
                    os.makedirs(dest_plugin_base)
                    with open(os.path.join(dest_plugin_base, '__init__.py'), 'w') as f:
                        f.write('')

                # process each plugin
                for plugin in spec[namespace][collection][plugin_type]:
                    plugin_sig = '%s/%s' % (plugin_type, plugin)
                    if plugin_sig in seen:
                        raise ValueError(
                            'Each plugin needs to be assigned to one collection '
                            f'only. {plugin_sig} has already been processed as a '
                            f'part of `{seen[plugin_sig]}` collection.'
                        )
                    seen[plugin_sig] = collection

                    # TODO: currently requires 'full name of file', but should work w/o extension?
                    relative_src_plugin_path = os.path.join(src_plugin_base, plugin)
                    src = os.path.join(checkout_path, relative_src_plugin_path)

                    remove(src)

                    if plugin_type in ('modules',) and '/' in plugin:
                        init_py_path = os.path.join(checkout_path, src_plugin_base, os.path.dirname(plugin), '__init__.py')
                        if os.path.exists(init_py_path):
                            remove(init_py_path)

                    do_preserve_subdirs = (
                        (args.preserve_module_subdirs and plugin_type == 'modules')
                        or plugin_type == 'module_utils'
                    )
                    plugin_path_chunk = plugin if do_preserve_subdirs else os.path.basename(plugin)
                    relative_dest_plugin_path = os.path.join(relative_dest_plugin_base, plugin_path_chunk)

                    migrated_to_collection[relative_src_plugin_path] = relative_dest_plugin_path

                    dest = os.path.join(collection_dir, relative_dest_plugin_path)
                    if do_preserve_subdirs:
                        os.makedirs(os.path.dirname(dest), exist_ok=True)

                    if not os.path.exists(src):
                        raise Exception('Spec specifies "%s" but file "%s" is not found in checkout' % (plugin, src))

                    if os.path.islink(src):
                        real_src = os.readlink(src)

                        # remove destination if it already exists
                        if os.path.exists(dest):
                            # NOTE: not atomic but should not matter in our script
                            logger.warning('Removed "%s" as it is target for symlink of "%s"' % (dest, src))
                            os.remove(dest)

                        if real_src.startswith('../'):
                            target = real_src[3:]
                            found = False
                            for k in spec[namespace][collection][plugin_type]:
                                if k.endswith(target):
                                    found = True
                                    break

                            if found:
                                if plugin_type == 'module_utils':
                                    target = real_src
                                else:
                                    target = os.path.basename(real_src)
                                ret = os.getcwd()
                                os.chdir(os.path.dirname(dest))
                                os.symlink(os.path.basename(target), os.path.basename(dest))
                                os.chdir(ret)
                            else:
                                raise Exception('Found symlink "%s" to target "%s" that is not in same collection.' % (src, target))
                        else:
                            shutil.copyfile(src, dest, follow_symlinks=False)

                        # dont rewrite symlinks, original file should already be handled
                        continue

                    elif not src.endswith('.py'):
                        # its not all python files, copy and go to next
                        # TODO: handle powershell import rewrites
                        shutil.copyfile(src, dest)
                        continue

                    logger.info('Processing %s -> %s' % (src, dest))

                    mod_src_text, mod_fst = read_module_txt_n_fst(src)

                    import_deps += rewrite_imports(mod_fst, collection, spec, namespace, args)
                    try:
                        docs_deps += rewrite_doc_fragments(mod_fst, collection, spec, namespace, args)
                    except LookupError as err:
                        logger.info('%s in %s', err, src)

                    rewrite_class_property(mod_fst, collection, namespace, dest)

                    plugin_data_new = mod_fst.dumps()

                    if mod_src_text != plugin_data_new:
                        logger.info('rewriting plugin references in %s' % dest)

                    write_text_into_file(dest, plugin_data_new)

                    integration_test_dirs.extend(poor_mans_integration_tests_discovery(checkout_path, plugin_type, plugin))
                    # process unit tests
                    unit_tests_migrated_to_collection = copy_unit_tests(
                        checkout_path, collection_dir,
                        plugin_type, plugin, spec,
                    )
                    migrated_to_collection.update(unit_tests_migrated_to_collection)
                    # TODO: sanity tests?

            inject_init_into_tree(
                os.path.join(collection_dir, 'tests', 'unit'),
            )

            for file_path in itertools.chain.from_iterable(
                    (os.path.join(dp, f) for f in fn if f.endswith('.py'))
                    for dp, dn, fn in os.walk(os.path.join(collection_dir, 'tests', 'unit'))
            ):
                _unit_test_module_src_text, unit_test_module_fst = read_module_txt_n_fst(file_path)
                unit_deps += rewrite_imports(unit_test_module_fst, collection, spec, namespace, args)
                unit_deps += rewrite_unit_tests_patch(unit_test_module_fst, collection, spec, namespace, args, file_path)
                write_text_into_file(file_path, unit_test_module_fst.dumps())

            inject_gitignore_into_tests(collection_dir)

            inject_ignore_into_sanity_tests(
                checkout_path, collection_dir, migrated_to_collection,
            )

            # FIXME need to hack PyYAML to preserve formatting (not how much it's possible or how much it is work) or use e.g. ruamel.yaml
            try:
                rewrite_integration_tests(integration_test_dirs, checkout_path, collection_dir, namespace, collection, spec, args)
            except yaml.composer.ComposerError as e:
                logger.error(e)

            global integration_tests_deps
            for dep_ns, dep_coll in integration_tests_deps.union(import_deps + docs_deps + unit_deps):
                dep = '%s.%s' % (dep_ns, dep_coll)
                # FIXME hardcoded version
                galaxy_metadata['dependencies'][dep] = '>=1.0'

            inject_requirements_into_integration_tests(checkout_path, collection_dir)

            integration_test_dirs = []
            integration_tests_deps = set()

            # write collection metadata
            write_yaml_into_file_as_is(
                os.path.join(collection_dir, 'galaxy.yml'),
                galaxy_metadata,
            )

            # init git repo
            subprocess.check_call(('git', 'init'), cwd=collection_dir)
            subprocess.check_call(('git', 'add', '.'), cwd=collection_dir)
            subprocess.check_call(
                ('git', 'commit', '-m', 'Initial commit', '--allow-empty'),
                cwd=collection_dir,
            )

            mark_moved_resources(
                checkout_path, namespace, collection, migrated_to_collection,
            )

            if args.move_plugins:
                actually_remove(namespace, collection)

            global REMOVE
            REMOVE = set()


def publish_to_github(collections_target_dir, spec, *, gh_org, gh_app_id, gh_app_key_path):
    """Push all migrated collections to their Git remotes."""
    collections_base_dir = os.path.join(collections_target_dir, 'collections')
    collections_root_dir = os.path.join(
        collections_base_dir,
        'ansible_collections',
    )
    collection_paths_except_core = (
        (os.path.join(collections_root_dir, ns, coll), f'{ns}.{coll}')
        for ns, ns_val in spec.items()
        for coll in ns_val.keys()
        if not coll.startswith('_')
    )
    github_api = GitHubOrgClient(gh_app_id, gh_app_key_path, gh_org)
    for collection_dir, repo_name in collection_paths_except_core:
        git_repo_url = read_yaml_file(
            os.path.join(collection_dir, 'galaxy.yml'),
        )['repository']
        with contextlib.suppress(LookupError):
            git_repo_url = github_api.get_git_repo_write_uri(repo_name)
        logger.debug(
            'Using %s...%s Git URL for push',
            git_repo_url[:5], git_repo_url[-5:],
        )
        subprocess.check_call(
            ('git', 'push', '--force', git_repo_url, 'HEAD:master'),
            cwd=collection_dir,
        )


def push_migrated_core(vardir):
    releases_dir = os.path.join(vardir, 'releases')
    devel_path = os.path.join(releases_dir, 'migrated_core.git')

    if not os.path.exists(devel_path):
        subprocess.check_call(('git', 'clone', MIGRATED_DEVEL_URL, 'migrated_core.git'), cwd=releases_dir)
    else:
        subprocess.check_call(('git', 'checkout', DEVEL_BRANCH), cwd=devel_path)
        subprocess.check_call(('git', 'pull', '--rebase'), cwd=devel_path)

    # NOTE assumes the repo is not used and/or is locked while migration is running
    subprocess.check_call(('git', 'push', '--force', 'origin', DEVEL_BRANCH), cwd=devel_path)


def mark_moved_resources(checkout_dir, namespace, collection, migrated_to_collection):
    """Mark migrated paths in botmeta."""
    migrated_to = '.'.join((namespace, collection))
    botmeta_rel_path = '.github/BOTMETA.yml'
    botmeta_checkout_path = os.path.join(checkout_dir, botmeta_rel_path)
    close_related_issues = False

    botmeta = read_yaml_file(botmeta_checkout_path)

    botmeta_files = botmeta['files']
    botmeta_file_paths = botmeta_files.keys()
    botmeta_macros = botmeta['macros']

    transformed_path_key_map = {}
    for k in botmeta_file_paths:
        transformed_key = Template(k).substitute(**botmeta_macros)
        if transformed_key == k:
            continue
        transformed_path_key_map[transformed_key] = k

    for migrated_resource in migrated_to_collection:
        macro_path = transformed_path_key_map.get(
            migrated_resource, migrated_resource,
        )

        migrated_secion = botmeta_files.get(macro_path)
        if not migrated_secion:
            migrated_secion = botmeta_files[macro_path] = {}
        elif isinstance(migrated_secion, str):
            migrated_secion = botmeta_files[macro_path] = {
                'maintainers': migrated_secion,
            }

        migrated_secion['close'] = close_related_issues
        migrated_secion['migrated_to'] = migrated_to

    write_yaml_into_file_as_is(botmeta_checkout_path, botmeta)

    # Commit changes to the migrated Git repo
    subprocess.check_call(
        ('git', 'add', f'{botmeta_rel_path!s}'),
        cwd=checkout_dir,
    )
    subprocess.check_call(
        (
            'git', 'commit',
            '-m', f'Mark migrated {collection}',
            '--allow-empty',
        ),
        cwd=checkout_dir,
    )


##############################################################################
# Rewrite integration tests
##############################################################################

integration_tests_deps = set()

def integration_tests_add_to_deps(collection, dep_collection):
    if collection == dep_collection:
        return

    global integration_tests_deps
    integration_tests_deps.add(dep_collection)
    logger.debug("Adding %s.%s as a dep for %s.%s" % (dep_collection[0], dep_collection[1], collection[0], collection[1]))


def poor_mans_integration_tests_discovery(checkout_dir, plugin_type, plugin_name):
    # FIXME this might be actually enough for modules integration tests, at least for the most part
    if plugin_type != 'modules':
        return []

    files = glob.glob(os.path.join(checkout_dir, 'test/integration/targets', os.path.basename(os.path.splitext(plugin_name)[0])))
    for fname in files:
        logger.debug('Found integration tests for %s %s in %s' % (plugin_type, plugin_name, fname))

    return files


def rewrite_integration_tests(test_dirs, checkout_dir, collection_dir, namespace, collection, spec, args):
    # FIXME module_defaults groups

    for test_dir in test_dirs:
        for dirpath, dirnames, filenames in os.walk(test_dir):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                logger.debug(full_path)

                dest_dir = os.path.join(collection_dir,
                                        'tests',
                                        os.path.relpath(dirpath, os.path.join(checkout_dir, 'test')))
                if not os.path.exists(dest_dir):
                    os.makedirs(dest_dir)
                dest = os.path.join(dest_dir, filename)

                dummy, ext = os.path.splitext(filename)

                if ext in ('.py',):
                    # FIXME duplicate code from the 'main' function
                    mod_src_text, mod_fst = read_module_txt_n_fst(full_path)

                    import_dependencies = rewrite_imports(mod_fst, collection, spec, namespace, args)
                    try:
                        docs_dependencies = rewrite_doc_fragments(mod_fst, collection, spec, namespace, args)
                    except LookupError as err:
                        docs_dependencies = []
                        logger.info('%s in %s', err, full_path)
                    plugin_data_new = mod_fst.dumps()

                    for dep_ns, dep_coll in docs_dependencies + import_dependencies:
                        integration_tests_add_to_deps((namespace, collection), (dep_ns, dep_coll))

                    write_text_into_file(dest, plugin_data_new)
                elif ext in ('.ps1',):
                    # FIXME
                    pass
                elif ext in ('.yml', '.yaml'):
                    rewrite_yaml(full_path, dest, namespace, collection, spec, args)
                elif ext in ('.sh',):
                    rewrite_sh(full_path, dest, namespace, collection, spec, args)
                elif filename == 'ansible.cfg':
                    rewrite_ini(full_path, dest, namespace, collection, spec, args)
                else:
                    shutil.copy2(full_path, dest)

                remove(full_path)


def rewrite_sh(full_path, dest, namespace, collection, spec, args):
    sh_key_map = {
        'ANSIBLE_CACHE_PLUGIN': 'cache',
        'ANSIBLE_CALLBACK_WHITELIST': 'callback',
        'ANSIBLE_INVENTORY_CACHE_PLUGIN': 'cache',
        'ANSIBLE_STDOUT_CALLBACK': 'callback',
        'ANSIBLE_STRATEGY': 'strategy',
        '--become-method': 'become',
        '-c': 'connection',
        '--connection': 'connection',
    }

    contents = read_text_from_file(full_path)
    for key, plugin_type in sh_key_map.items():
        if not contents.find(key):
            continue
        for ns in spec.keys():
            for coll in get_rewritable_collections(ns, spec):
                plugins = get_plugins_from_collection(ns, coll, plugin_type, spec)
                for plugin_name in plugins:
                    if not contents.find(plugin_name):
                        continue
                    # FIXME list
                    new_plugin_name = get_plugin_fqcn(ns, coll, plugin_name)

                    if args.fail_on_core_rewrite:
                        raise RuntimeError('Rewriting to %s' % new_plugin_name)
                    contents = contents.replace(key + '=' + plugin_name, key + '=' + new_plugin_name)
                    contents = contents.replace(key + ' ' + plugin_name, key + ' ' + new_plugin_name)
                    integration_tests_add_to_deps((namespace, collection), (ns, coll))

    write_text_into_file(dest, contents)
    shutil.copystat(full_path, dest)


def rewrite_ini(src, dest, namespace, collection, spec, args):
    ini_key_map = {
        'defaults': {
            'callback_whitelist': 'callback',
            'fact_caching': 'cache',
            'strategy': 'strategy',
            'stdout_callback': 'callback',
        },
        'inventory': {
            'cache_plugin': 'cache',
            'enable_plugins': 'inventory',
        }
    }

    config = configparser.ConfigParser()
    config.read(src)
    for section in config.sections():
        try:
            rewrite_ini_section(config, ini_key_map, section, namespace, collection, spec, args)
        except KeyError:
            continue

    with open(dest, 'w') as cf:
        config.write(cf)


def rewrite_ini_section(config, key_map, section, namespace, collection, spec, args):
    for keyword, plugin_type in key_map[section].items():
        try:
            # FIXME diff input format than csv?
            plugin_names = config.get(section, keyword).split(',')
        except configparser.NoOptionError:
            continue

        new_plugin_names = []
        for plugin_name in plugin_names:
            try:
                plugin_namespace, plugin_collection = get_plugin_collection(plugin_name, plugin_type, spec)
                if plugin_collection in COLLECTION_SKIP_REWRITE:
                    raise LookupError
                if args.fail_on_core_rewrite:
                    raise RuntimeError('Rewriting to %s.%s.%s' % (plugin_namespace, plugin_collection, plugin_name))
                new_plugin_names.append(get_plugin_fqcn(namespace, plugin_collection, plugin_name))
                integration_tests_add_to_deps((namespace, collection), (plugin_namespace, plugin_collection))
            except LookupError:
                new_plugin_names.append(plugin_name)

        config.set(section, keyword, ','.join(new_plugin_names))


def rewrite_yaml(src, dest, namespace, collection, spec, args):
    try:
        contents = read_ansible_yaml_file(src)
        _rewrite_yaml(contents, namespace, collection, spec, args, dest)
        write_ansible_yaml_into_file_as_is(dest, contents)
    except Exception as e:
        logger.error('Skipping bad YAML in %s: %s' % (src, str(e)))


def _rewrite_yaml(contents, namespace, collection, spec, args, dest):
    if isinstance(contents, list):
        for el in contents:
            _rewrite_yaml(el, namespace, collection, spec, args, dest)
    elif isinstance(contents, Mapping):
        _rewrite_yaml_mapping(contents, namespace, collection, spec, args, dest)


def _rewrite_yaml_mapping(el, namespace, collection, spec, args, dest):
    assert isinstance(el, Mapping)

    _rewrite_yaml_mapping_keys(el, namespace, collection, spec, args, dest)
    _rewrite_yaml_mapping_keys_non_vars(el, namespace, collection, spec, args)
    _rewrite_yaml_mapping_values(el, namespace, collection, spec, args, dest)


KEYWORD_TO_PLUGIN_MAP = {
    'ansible_become_method': 'become',
    'ansible_connection': 'connection',
    'ansible_shell_type': 'shell',
    'become_method': 'become',
    'cache_plugin': 'cache',
    'connection': 'connection',
    'plugin': 'inventory',
    'strategy': 'strategy',
}


def _rewrite_yaml_mapping_keys_non_vars(el, namespace, collection, spec, args):
    translate = []
    for key in el.keys():
        if is_reserved_name(key):
            continue

        prefix = 'with_'
        if prefix in key:
            prefix_len = len(prefix)

            if not key.startswith(prefix):
                continue
            plugin_name = key[prefix_len:]
            try:
                plugin_namespace, plugin_collection = get_plugin_collection(plugin_name, 'lookup', spec)
                if plugin_collection in COLLECTION_SKIP_REWRITE:
                    raise LookupError

                if args.fail_on_core_rewrite:
                    raise RuntimeError('Rewriting to %s.%s.%s' % (plugin_namespace, plugin_collection, plugin_name))
                translate.append((prefix + get_plugin_fqcn(namespace, plugin_collection, plugin_name), key))
                integration_tests_add_to_deps((namespace, collection), (plugin_namespace, plugin_collection))
            except LookupError:
                pass

        for ns in spec.keys():
            for coll in get_rewritable_collections(ns, spec):
                try:
                    modules_in_collection = get_plugins_from_collection(ns, coll, 'modules', spec)
                except LookupError:
                    continue

                for module in modules_in_collection:
                    if key != module:
                        continue
                    new_module_name = get_plugin_fqcn(ns, coll, key)
                    if args.fail_on_core_rewrite:
                        raise RuntimeError('Rewriting to %s' % new_module_name)
                    translate.append((new_module_name, key))
                    integration_tests_add_to_deps((namespace, collection), (ns, coll))

    for new_key, old_key in translate:
        el[new_key] = el.pop(old_key)


def _rewrite_yaml_mapping_keys(el, namespace, collection, spec, args, dest):
    for key in el.keys():
        if is_reserved_name(key):
            continue

        plugin_type = KEYWORD_TO_PLUGIN_MAP.get(key)
        if plugin_type is None:
            continue

        try:
            plugin_namespace, plugin_collection = get_plugin_collection(el[key], plugin_type, spec)
            if plugin_collection in COLLECTION_SKIP_REWRITE:
                continue
            new_plugin_name = get_plugin_fqcn(namespace, plugin_collection, el[key])
            if args.fail_on_core_rewrite:
                raise RuntimeError('Rewriting to %s' % new_plugin_name)
            el[key] = new_plugin_name
            integration_tests_add_to_deps((namespace, collection), (plugin_namespace, plugin_collection))
        except LookupError:
            if '{{' in el[key]:
                add_manual_check((key, el[key], dest))


def _rewrite_yaml_mapping_values(el, namespace, collection, spec, args, dest):
    for key, value in el.items():
        if isinstance(value, Mapping):
            if key == 'vars':
                _rewrite_yaml_mapping_keys(el[key], namespace, collection, spec, args, dest)
            if key != 'vars':
                _rewrite_yaml_mapping_keys_non_vars(el[key], namespace, collection, spec, args)
        elif isinstance(value, list):
            for idx, item in enumerate(value):
                if isinstance(item, Mapping):
                    _rewrite_yaml_mapping(el[key][idx], namespace, collection, spec, args, dest)
                else:
                    if key == 'module_blacklist':
                        for ns in spec.keys():
                            for coll in get_rewritable_collections(ns, spec):
                                if item in get_plugins_from_collection(ns, coll, 'modules', spec):
                                    new_plugin_name = get_plugin_fqcn(ns, coll, el[key][idx])
                                    if args.fail_on_core_rewrite:
                                        raise RuntimeError('Rewriting to %s' % new_plugin_name)
                                    el[key][idx] = new_plugin_name
                                    integration_tests_add_to_deps((namespace, collection), (ns, coll))
                    if isinstance(el[key][idx], str):
                        # FIXME move to a func
                        el[key][idx] = _rewrite_yaml_lookup(el[key][idx], namespace, collection, spec, args)
                        el[key][idx] = _rewrite_yaml_filter(el[key][idx], namespace, collection, spec, args)
                        el[key][idx] = _rewrite_yaml_test(el[key][idx], namespace, collection, spec, args)
        elif isinstance(value, str):
            el[key] = _rewrite_yaml_lookup(el[key], namespace, collection, spec, args)
            el[key] = _rewrite_yaml_filter(el[key], namespace, collection, spec, args)
            el[key] = _rewrite_yaml_test(el[key], namespace, collection, spec, args)


def _rewrite_yaml_lookup(value, namespace, collection, spec, args):
    if not ('lookup(' in value or 'query(' in value or 'q(' in value):
        return value

    for ns in spec.keys():
        for coll in get_rewritable_collections(ns, spec):
            for plugin_name in get_plugins_from_collection(ns, coll, 'lookup', spec):
                if plugin_name not in value:
                    continue
                new_plugin_name = get_plugin_fqcn(ns, coll, plugin_name)
                if args.fail_on_core_rewrite:
                    raise RuntimeError('Rewriting to %s' % new_plugin_name)
                value = value.replace(plugin_name, new_plugin_name)
                integration_tests_add_to_deps((namespace, collection), (ns, coll))

    return value


def _rewrite_yaml_filter(value, namespace, collection, spec, args):
    if '|' not in value:
        return value
    for ns in spec.keys():
        for coll in get_rewritable_collections(ns, spec):
            for filter_plugin_name in get_plugins_from_collection(ns, coll, 'filter', spec):
                imported_module = import_module('ansible.plugins.filter.' + filter_plugin_name)
                fm = getattr(imported_module, 'FilterModule', None)
                if fm is None:
                    continue
                filters = fm().filters().keys()
                for found_filter in (match[5] for match in FILTER_RE.findall(value)):
                    if found_filter not in filters:
                        continue
                    new_plugin_name = get_plugin_fqcn(ns, coll, found_filter)
                    if args.fail_on_core_rewrite:
                        raise RuntimeError('Rewriting to %s' % new_plugin_name)
                    value = value.replace(found_filter, new_plugin_name)
                    integration_tests_add_to_deps((namespace, collection), (ns, coll))

    return value


def _rewrite_yaml_test(value, namespace, collection, spec, args):
    if ' is ' not in value:
        return value
    for ns in spec.keys():
        for coll in get_rewritable_collections(ns, spec):
            for test_plugin_name in get_plugins_from_collection(ns, coll, 'test', spec):
                imported_module = import_module('ansible.plugins.test.' + test_plugin_name)
                tm = getattr(imported_module, 'TestModule', None)
                if tm is None:
                    continue
                tests = tm().tests().keys()
                for found_test in (match[5] for match in TEST_RE.findall(value)):
                    if found_test not in tests:
                        continue
                    new_plugin_name = get_plugin_fqcn(ns, coll, found_filter)
                    if args.fail_on_core_rewrite:
                        raise RuntimeError('Rewriting to %s' % new_plugin_name)
                    value = value.replace(found_test, new_plugin_name)
                    integration_tests_add_to_deps((namespace, collection), (ns, coll))

    return value


##############################################################################
# Rewrite integration tests END
##############################################################################


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--spec', required=True, dest='spec_dir',
                        help='A directory spec with YAML files that describe how to organize collections')
    parser.add_argument('-r', '--refresh', action='store_true', dest='refresh', default=False,
                        help='force refreshing local Ansible checkout')
    parser.add_argument('-t', '--target-dir', dest='vardir', default=VARDIR,
                        help='target directory for resulting collections and rpm')
    parser.add_argument('-p', '--preserve-module-subdirs', action='store_true', dest='preserve_module_subdirs', default=False,
                        help='preserve module subdirs per spec')
    parser.add_argument(
        '--github-app-id',
        action='store',
        type=int,
        dest='github_app_id',
        default=41435,
        help='Use this GitHub App ID for GH auth',
    )
    parser.add_argument(
        '--github-app-key-path',
        action='store',
        type=str,
        dest='github_app_key_path',
        default='~/Downloads/ansible-migrator.2019-09-18.private-key.pem',
        help='Use this PEM key file for GH auth',
    )
    parser.add_argument(
        '--target-github-org',
        action='store',
        type=str,
        dest='target_github_org',
        default='ansible-collections',
        help='Push migrated collections to this GH org',
    )
    parser.add_argument(
        '-P',
        '--publish-to-github',
        action='store_true',
        dest='publish_to_github',
        default=False,
        help='Push all migrated collections to their Git remotes'
    )
    parser.add_argument('-m', '--move-plugins', action='store_true', dest='move_plugins', default=False,
                        help='remove plugins from source instead of just copying them')
    parser.add_argument('-M', '--push-migrated-core', action='store_true', dest='push_migrated_core', default=False,
                        help='Push migrated core to the Git repo')
    parser.add_argument('-f', '--fail-on-core-rewrite', action='store_true', dest='fail_on_core_rewrite', default=False,
            help='Fail on core rewrite. E.g. to verify core does not depend on the collections by running migration against the list of files kept in core: spec must contain the "_core" collection.')

    args = parser.parse_args()

    # required, so we should always have
    spec = {}

    for spec_file in os.listdir(args.spec_dir):
        try:
            spec[os.path.splitext(os.path.basename(spec_file))[0]] = load_spec_file(os.path.join(args.spec_dir, spec_file))
        except Exception as e:
            # warn we skipped spec_file for reasons: e
            raise

    checkout_repo(args.vardir, args.refresh)

    # doeet
    assemble_collections(spec, args, args.target_github_org)

    if args.publish_to_github:
        publish_to_github(
            args.vardir, spec,
            gh_org=args.target_github_org,
            gh_app_id=args.github_app_id,
            gh_app_key_path=args.github_app_key_path,
        )

    if args.push_migrated_core:
        push_migrated_core(args.vardir)

    global core
    print('======= Assumed stayed in core =======\n')
    print(yaml.dump(core))

    global manual_check
    print('======= Could not rewrite the following, please check manually =======\n')
    print(yaml.dump(manual_check))

if __name__ == "__main__":
    main()
