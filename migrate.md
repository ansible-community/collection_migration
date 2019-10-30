migrating to collections
========================

Traditionally there were 2 ways to distribute plugins (including modules) for an Ansible installation,
use a role and then import it inside a play or install the plugin where ansible would find it (adjacent to play or in configured directories).

Collections allow for a much looser and easier reference to plugins and modules and a more consolidated path to install as well as better internal reference to avoid conflicts.
They also allow roles and other content you might want to distribute.

Also you will become familiar with FQCN (Full Qualified Collection Name) for each plugin type as this will need to be used both in the migration and in plays using the collection.


This project is using migrate.py to do automated migration from ansible/ansible to separate repos that can host 1 or more collections per repo, but there are those that might want to do a manual migration themselves.

So how do you move existing code into a collection? Below we describe the steps that migrate.py does automatically but might not align with the structure you might want for your content.


galaxy.yml
==========

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

 .github/BOTMETA.yml
====================

This file is used by the core issue management bot and other tools to keep track of many things, we added a `migrated_to` field that will indicate the collection name of where a file is supposed to primarily reside now.
You only need to update this file if your content is currently in ansible/ansible and can be ignored for 3rd party distributed content.

migrating plugins
=================

The modules and the other plugin types require some rewrites to support collection semantics, mostly do deal with how they reference other resources, mostly module_utils and doc_fragments.


 - paths:
        - The first thing to note, all runtime code is under the ``plugins/`` path, so modules, module_utils and all other plugins are in their own plugin specific directory under the plugins/ top level directory in the collection.
          This is similar to what already exists in roles, but for modules and module_utils it is a change from the core repo.
        - tests now live under the ``tests/`` directory with ``units`` and ``integration`` subdirectories.

 - Collection roles exist under the roles/ directory, with a couple of restrictions:
        - They cannot have their own plugins, so  plugins in role adjacent directories will be ignored
        - Role 'dependencies' will not be installed on collection install, if using collections those dependencies should be specified in galaxy.yml

 - for most plugins you need to rewrite:
        - documentation:
                - doc_fragments: must use FQCN, examine all modules and plugins that use doc_fragments and rewrite from 'myfrag' to 'myname.mycoll.myfrag'.
                - general docs: any references to modules and other plugins need to be rewritten to use FQCN, normally found in descriptions and examples.

        - imports (also for tests):
                - relative imports work in collections, but only if they start with a ``.`` so ``./filename`` and ``../asdfu/filestuff`` works but ``filename`` in same dir does not.
                - module_utils: the most obvious one, you go from ``import ansible.module_utils.randomdir.myutil`` to ``import ansible_collections.myname.mycoll.randomdir.myutil``.
                - subclassing plugins: you need to follow the same rules in changing paths and using namespaced names.

        - patches and mocks: mostly used in tests, these also might need rewrites with the same rules as imports follow.

        - sanity tests: ignores need to be moved to ansible specific version ignores files ignores-2.9.txt

        - integration tests and roles:
                - tasks need to be rewritten to use FQCN, this includes modules, lookups, filters and any other plugin referenced that has been moved to a collection.
                - Jinja filters and tests FQCN includes the file name where the plugin resides.


There are many corner cases if you want to automate the points above, see the `migrate.py` script for specifics.
