## Aim

bcoca's opinionated 'minimal' testable Ansible version, keeps certain things in Core that are either required for any Ansible cli to execute or allow minimal configuration of the system to then enable installing collections and/or other content.

This is *NOT *for final/production use, just meant as a good test bed for manualy migrated collections and to narrow down what core subsystems require to function.

## Use Cases

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
