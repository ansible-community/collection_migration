[![GitHub Actions CI/CD build status — Collection migration smoke test suite](https://github.com/ansible-community/collection_migration/workflows/CI/CD/badge.svg?branch=master)](https://github.com/ansible-community/collection_migration/actions?query=workflow%3ACI%2FCD+branch%3Amaster)


migrate.py
==========

The migration script (`migrate.py`) has incorporated help, so using
`--help` you can see all the options.


Runtime pre-requisites
----------------------

The main requisite is **Python 3.7**. You'll also need to install
the dependency packages:

```console
$ python3.7 -m venv .venv
$ . .venv/bin/activate
(.venv) $ python3.7 -m pip install -r requirements.in
```

*Pro tip:* If you wish to install exactly the same dep versions
as the CI uses, make sure to add `-c requirements.txt`
in the end of that last command.


Migration scenario
------------------

The script takes a scenario as a mandatory argument
(`-s path/to/dir`), a scenario is a directory with one or more YAML
files that describe the collection layout post migration.

Each file name is the namespace of the included collections, inside
you can have a collection name, followed by the plugin types and
actual plugin files (with extensions) in that collection as they
appear in the ansible repo, including subdirectories from their
expected locations. For example:

```yaml
# test_scenario/microsoft.yml
azure:
  module_utils:
  - azure.py
  - azure_rm.py
  modules:
  - cloud/azure/azure_rm_instance.py
windows:
  lookups:
  - win_registry.py
```

Some existing scenarios are already provided in the repo, `bare` and
`minimal` being the most useful ones as they can generate (with `-m`
option) an ansible repo w/o most of the plugins (`bare` has none,
`minimal` has the ones we have considered Ansible requires to
minimally function).


Performing the migration
------------------------

In order to run the migration based on the existing scenario, you'd
need to execute somehting like this:

```console
(.venv) $ python3.7 -m migrate -s scenarios/minimal
```


Generating a bare scenario
--------------------------

Another useful script is `generate_glob_collection.sh` which outputs
a YAML structure to stdout that lists ALL the plugins from an Ansible
checkout (which is the only required parameter), useful to regenerate
the `bare` scenario or as a starting point for other scenarios.


Note: scenarios support 'informative' collections, that start with `_`
as a means to let collections know dependencies but not actually
migrate, also the special `_core` collection is used to indicate
plugins that would stay in core and not require rewrites for those
referencing them.


Things to be aware of
---------------------

* If the scenario doesn't contain an explicit enumeration of artifacts
  related to the given resource, it may result in an incomplete
  migration.
  One example of such case it including an action plugin and omitting
  the module with the same name, or any other related files. This may
  result in various sanity and/or other tests failures.
  E.g. `action plugin has no matching module to provide documentation`
  (`action-plugin-docs`).
  
Definitions for the 2.10 Ansible Release
----------------------------------------

There are a few terms that are important to the understanding of the
Ansible 2.10 release, that impact and indicate how ansible will be
structured, and distributed.

stdlib
: The bare essentials needed to make Ansible functional (needs feature list),
  while providing no ability to perform work against a target host.
  This will largely be a release or distribution with little to no plugins or modules.
  - current state: needs work, currently we remove all plugins but that
     leaves Ansible in unusable state, needs a lot of work to start adding
     things back until the 'base functionality'is working again.

base
: This uses stdlib as it's foundation, while also including a small number
  or plugins and modules that roughly track the 2.9 definition of "core"
  supported plugins and modules. This will provide a limited functionality
  to support a standard use case that may involve bootstrapping a host,
  to a point where additionall collections can then be used.
  - current state: not really derived off stdlib as that is currently unusable,
    using bcs scenario as 'candidate' and keeping base itself as a symlinnk to
    easily change across candidates.
