The migration script (migrate.py) has incorporated help, so using --help you can see all the options.

The script takes a scenario as a mandatory argument (`-s path/to/dir`), a scenario is a directory with one or more YAML files that describe the collection layout post migration.

Each file name is the namespace of the included collections, inside you can have a collection name, followed by the plugin types and actual plugin files (with extensions) in that collection as they appear in the ansible repo, including subdirectories from their expected locations::


test_scenario/ microsoft.yml
```
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

Some existing scenarios are already provided in the repo, `bare` and `minimal` being the most useful ones as they can generate (with `-m` option) an ansible repo w/o most of the plugins (bare has none, minimal has the ones we have considered Ansible requires to minimally function).

Another useful script is `generate_glob_collection.sh` which outputs a YAML structure to stdout that lists ALL the plugins from an Ansible checkout (which is only required parameter), useful to regenerate the 'bare' scenario or as a starting point for other scenarios.


Note: scenarios support 'informative' collections, that start with `_` as a means to let collections know dependencies but not actually migrate, also the special `_core` collection is used to indicate plugins that would stay in core and not require rewrites for those referencing them.
