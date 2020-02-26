# migrating to collections

Traditionally there were 2 ways to distribute plugins (including modules) for an Ansible installation,
use a role and then import it inside a play or install the plugin where ansible would find it (adjacent to play or in configured directories).

Collections allow for a much looser and easier reference to plugins and modules and a more consolidated path to install as well as better internal reference to avoid conflicts.
They also allow roles and other content you might want to distribute.

Also you will become familiar with FQCN (Full Qualified Collection Name) for each plugin type as this will need to be used both in the migration and in plays using the collection.


This project is using migrate.py to do automated migration from ansible/ansible to separate repos that can host 1 or more collections per repo, but there are those that might want to do a manual migration themselves.

So how do you move existing code into a collection? Below we describe the steps that migrate.py does automatically but might not align with the structure you might want for your content.


# galaxy.yml

The first thing is creating the file that 'defines' your collection, this should have the namespace and collection name as well as any other collections you depend on for content.
( note: if you plan to distribute via galaxy or some other service you need to claim that namespace). This file is used by the build process to create the collection package.


example file:
```
namespace: myname
name: mycoll
version: '1.0.0'
readme: null
authors: null
description: something
license: GPLv3+
tags: [stuff, morestuff, mystuff]
dependencies: null # collections i depend on
repository: null # optionally repo where this collection lives
documentation: null
homepage: null
issues: null
```

# .github/BOTMETA.yml

This file is used by the core issue management bot and other tools to keep track of many things, we added a `migrated_to` field that will indicate the collection name of where a file is supposed to primarily reside now.
You only need to update this file if your content is currently in ansible/ansible and can be ignored for 3rd party distributed content.

# migrating plugins

The modules and the other plugin types require some rewrites to support collection semantics, mostly do deal with how they reference other resources, mostly module_utils and doc_fragments.


## Paths
The first thing to note, all runtime code is under the ``plugins/`` path, so modules, module_utils and
all other plugins are in their own plugin specific directory under the plugins/ top level directory in the collection.
This is similar to what already exists in roles, but for modules and module_utils it is a change from the core repo.

Tests now live under the ``tests/`` directory with ``units`` and ``integration`` subdirectories.

## Roles
Collection roles exist under the roles/ directory, with a couple of restrictions.
 
* They cannot have their own plugins, so  plugins in role adjacent directories will be ignored
* Role 'dependencies' will not be installed on collection install, if using collections those dependencies should be specified in galaxy.yml

## Plugins

for most plugins you need to rewrite.
 
### documentation
* doc_fragments: must use FQCN, examine all modules and plugins that use doc_fragments and rewrite from 'myfrag' to 'myname.mycoll.myfrag'.
		
* general docs: any references to modules and other plugins need to be rewritten to use FQCN, normally found in descriptions and examples.

### imports (also for tests).
	
* relative imports work in collections, but only if they start with a ``.`` so ``./filename`` and ``../asdfu/filestuff`` works but ``filename`` in same dir does not.
		
* module_utils: the most obvious one, you go from ``import ansible.module_utils.randomdir.myutil`` to ``import ansible_collections.myname.mycoll.randomdir.myutil``.
		
	* subclassing plugins: you need to follow the same rules in changing paths and using namespaced names.

* patches and mocks: mostly used in tests, these also might need rewrites with the same rules as imports follow.

* sanity tests: ignores need to be moved to ansible specific version ignores files ignores-2.9.txt

* integration tests and roles:
  * tasks need to be rewritten to use FQCN, this includes modules, lookups, filters and any other plugin referenced that has been moved to a collection.
  * Jinja filters and tests FQCN includes the file name where the plugin resides.


* BOTMETA.yml (discussed above for all files)

* lib/ansbile/config/routing.yml entry for 'trasnparent execution' for plugins that used to be in core, hopefully to avoid rewritting all exising playbooks.

* new meta/routing.yml in collections that now handle 'aliases across collections', deprecations, plugin removals.

* new meta/action_groups.yml in collections that now handle module_defaults 'action to group' mappings, no more need to update in core.


Things to consider after migration.
===================================

migrate.py does not handle everything, just most of what could be automated, here is a list of things you might need to do manually aftewards.


free form references
--------------------

Documentation, comments and other 'free form' references to plugins/modules will need manual updates, for example, `EXAMPLES` and `description` sections in plugin docs.


git history
-----------

The existing git history is not being moved to the new repos, they all start as a new commit, we had plans to do so, but were not able to realize due to the time table.
There are existing scripts from the last 'repo merge/splits' (ansible-modules-core/ansilbe-modules-extras) that still exist and give a blueprint on how we did preserve history in the past.
New repo owners can use these as examples/guides if they wish to reintegrate past history into the new repos after migration (check hacking/ history in core).


plugin/module_utils imports
---------------------------

Currently these will be broken for 3rd party plugin imports that reference files that used to live in core, there is work being done to transparently map to the new colleciton location but no code exists as of yet. Another option is to rewrite the imports to point to new collection locations. Anything that was in core should have already been rewritten to match the new locations.


ansible-doc
-----------

Currently it can show plugin docs from collections, but not list what plugins a collection provides (also being worked on).
ansible-galaxy recently added listing available collections and that code will be built on to list 'available plugins including collections'.


docs.ansible.com
----------------

Docs team has a system to show plugins from chosen collections, still in testing, should be ready soon after migration.


filters and tests docs
----------------------

These were still manualy written in docs.ansible.com and do not have any autogeneration, migration does not change this, all changes will still have to be manual.
