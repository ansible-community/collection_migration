# Ansible-base

## Use Cases for ansible-base

What `ansible/ansible` will become, ie `ansible-base`


* `ansible[|-playbook|-galaxy|-pull|-doc]` --help
* Being able to install content from Galaxy or Automation Hub
  * `ansible-galaxy collection ...`
  * Setup Networking
  * Setup Proxy
* Being able to install supported content via packages
  * ie RHEL users will not use `ansible-galaxy collection install ...`, they want RPMs
  * Ability to setup and use package repos
  * Ability to work online or offline
* Include things that are "hardcoded" into Ansible
  * eg `stat` is used to handle any file information internally
  * `include_tasks` is hardcoded as the implementation is inside the engine, same with `add_hosts`, `group-by`, `debug` and others, async_wrapp, async-poll, assert/fail are 'parts of the language'
* Development
  * Ability to run `ansible-test sanity,unit,integration` against the Ansible code base


## Windows details

Windows touches 5 main areas in the Ansible engine itself:
* Executor
* Shell plugins
* Connection plugins
* Action plugins
* Module utils

### Executor

This is the code used to build the "manifest" (metadata used to execute a module) and the code is currently located at [lib/ansible/executor/powershell](https://github.com/ansible/ansible/tree/devel/lib/ansible/executor/powershell). This code is is what implements become, async, coverage (ansible-test) for Windows hosts and it's [entrypoint](https://github.com/ansible/ansible/blob/86663abf371d8352498554ab72c00c55890b5588/lib/ansible/executor/powershell/module_manifest.py#L259-L262) requires a lot of information about the task itself to achieve this. Taking this outside of the Ansible codebase would require us to create an interface we can rely on being stable as right now it's just kept in lock step with whatever calls it and is tested in CI.
Another component of the executor is the various powershell wrapper scripts that are part of the manifest sent to the Windows host. These scripts contain the logic used by PowerShell to execute the module and return the data back to the controller. They have the ability to utilise existing PowerShell and C# module_utils to reduce code duplication which is critical due to the code complexity of these utils. Splitting them out means bugfixes in 1 would have to be transferred to the other which is not easy due to the code complexity and modules could potentially loose out of extra functionality that we add in the other.

### Shell

There are 2 shell plugins for Windows; [powershell](https://github.com/ansible/ansible/blob/devel/lib/ansible/plugins/shell/powershell.py) and [cmd](https://github.com/ansible/ansible/blob/devel/lib/ansible/plugins/shell/cmd.py). The latter is mostly just an implementation of powershell.py and is used for SSH support on Windows so its reasons are the same for powershell's reasons.
The powershell shell plugin contains a few methods that are sprinkled throughout the Ansible codebase like [script](https://github.com/ansible/ansible/blob/faaa669764faba8f2a1b4292afc3bc494e4a1932/lib/ansible/plugins/action/script.py#L129), [wait_for_connection](https://github.com/ansible/ansible/blob/68428efc39313b7fb22b77152ec548ca983b03dd/lib/ansible/plugins/action/wait_for_connection.py#L89), [ssh](https://github.com/ansible/ansible/blob/9a13d56b266a8180726d561d64076c4bf8f95fb9/lib/ansible/plugins/connection/ssh.py#L462), and even [action/__init__.py](https://github.com/ansible/ansible/blob/9b7198d25ecf084b6a465ba445efd426022265c3/lib/ansible/plugins/action/__init__.py#L485). This has resulted in a coupling of code in the engine that uses the shell plugin to control its behaviour depending on whether we are targeting a Windows host or not. Without a standard interface to determine this information we cannot safely rely on these private attributes/methods being available in all collection versions leading to a dependency hell like scenario when using Ansible against Windows.

### Connection

Currently we have 3 different connection plugins that can be used for Windows; `ssh`, `winrm`, and `psrp` (there are other community ones that I cannot speak to here). The `ssh` plugin is already in base so we can skip that. The other 2 are very similar and both have tight coupling with the powershell.py shell plugin. The biggest issue with connection plugins for Windows is its tight coupling with the shell plugins. We cannot safely split them up and due to the reasons above for `Shell` we need to keep those plugins within Ansible for now.
Once shell plugins are safely decoupled from Ansible there is less of a reason to keep these as part of the engine but until then we cannot split the Windows connection and shell plugins from each other.

### Action

Some action plugins like `win_reboot` and `win_template` just implement the POSIX plugin counterpart and aren't too complex but something like `win_copy` and `win_updates` are quite complex and interact with things like the become plugin set on the connection and execute multiple modules or other action plugins. My biggest concern is that accessing things like the connection plugin or even executing modules are behind a private function or attribute. Having this split out just makes them more brittle and I don't feel comfortable ripping them out until a more solid interface for these scenarios have been implemented.

### Module utils

There are 2 types of module utils used by Windows, [csharp](https://github.com/ansible/ansible/tree/devel/lib/ansible/module_utils/csharp), and [powershell](https://github.com/ansible/ansible/tree/devel/lib/ansible/module_utils/powershell) utils.

#### CSharp utils

A lot of these utils are for very low level functions in Windows and are fundamental to a lot of module actions and we want to have fine control over what they do. With the exception of `Ansible.Privilege.cs` and `Ansible.Basic.cs`, all the C# utils are used by the executor in some form or another. This is the prime reason why they should still be shipped with Ansible. Stripping it out would mean we could be introducing changes in a collection that is incompatible with an Ansible release breaking fundamental things like become or async. The `Ansible.Basic.cs` util isn't necessarily used by the executor but it is a core part of running a module like `basic.py`. We even have ansible-test sanity tests that utilise that util to do things like validate the module arg spec.


#### Powershell utils

The PowerShell module utils are less critical than the C# ones but some these 2 should be kept in Ansible;
* `AddType.psm1` - Used by the executor to compile the C# utils and potentially add support for C# modules in the future
* `Legacy.psm1` - The older version of the C# `Ansible.Basic.cs`, still used by lots of 3rd party modules (as well as win modules in Asible) and the same reasons for `Ansible.Basic.cs` apply here
The remaining ones are purely module side and can be put in it's own collection. The biggest downside is that 3rd party modules now have a "hidden" dependency of the target Windows collection in future Ansible versions.

### Modules

Some of the Windows modules are used by ansible-test helpers, for example:

* [`test/lib/ansible_test/_data/playbooks/windows_coverage_setup.yml`](https://github.com/ansible/ansible/blob/devel/test/lib/ansible_test/_data/playbooks/windows_coverage_setup.yml) - uses `win_file` and `win_acl` to setup directories to enable code coverage
* [`test/integration/targets/setup_remote_tmp_dir/tasks/windows.yml`](https://github.com/ansible/ansible/blob/devel/test/integration/targets/setup_remote_tmp_dir/tasks/windows.yml) - uses `win_tempfile` to generate unique working director for tests
* and others...

## Missing from ansible-minimal

Rough notes on other changes needed

* `action/script` (has module)
* `wait_for_connection` - Think this is used by ansible-test to check that machines have come up
