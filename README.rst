#######################
Python project template
#######################

Create your new Python project by forking this repo. This way you start with
a number of helpful development tools set up for you. These tools are:

* `Poetry build system <https://python-poetry.org/>`_
* `Tox testing <https://tox.wiki/en/latest/>`_
* `Pre-commit hooks <https://pre-commit.com/>`_ for formatting, linting,
  not only Python but also YAML and rST
* `Tests with pytest <https://docs.pytest.org/>`_
* `Bump2version <https://github.com/c4urself/bump2version>`_ version
  increment tool
* Documentation build with `Sphinx <https://www.sphinx-doc.org/en/master/>`_

This repo is part of the effort towards improving development practices in
our group. For more information on these aims and the tools themselves, check
out the `Efficient development <https://github.com/saezlab/HowTo/blob/master\
/HowTo_efficient_development.md>`_ page of the group Howto, and the Tools
page in `this spreadsheet <https://docs.google.com/spreadsheets/d\
/1by744ceMxt57egyq8Q4SZUfzLRhNG6BtlpoD3ljgp90/edit#gid=0>`_.

Setup
=====

Project name
------------

Find a name for your project. Below we use the placeholder ``new_project``.

Install tools
-------------

Make sure the necessary management tools are installed on your system:

.. code:: bash

   curl -sSL https://install.python-poetry.org | python3 -
   pip install --user tox pre-commit

Copy the template
-----------------

Clone the repo in your new project directory, and enter the directory:

.. code:: bash

   git clone https://github.com/saezlab/python_project new_project
   cd new_project

Project git repo
----------------

Create a repo for your project and make the cloned repo point to this repo:

.. code:: bash

   git remote origin set-url git@github.com:saezlab/new_project

Rename it
---------

Change the placeholder ``project_name`` used in this template in all files.

.. note::

   If you are on a non-BSD system (e.g. GNU), remove the first argument for
   ``sed -i``. That is the backup file extension, an argument that does not
   exist on GNU systems.

.. code:: bash

   find . -depth -type f -exec sed -i '' 's/project_name/project_name/g' {} +
   git add -u
   git commit -nm 'set project name'

Add your name
-------------

Change the author name and email in ``pyproject.toml`` and the headers of
all files in the module directory. Commit the changes.

.. code:: bash

   find . -depth -type f -exec sed -i '' 's/Denes Turei/Your Name/g' {} +
   find . -depth -type f -exec sed -i '' 's/turei\.denes@gmail\.com/your@email/g' {} +
   git add -u
   git commit -nm 'set author'

License
-------

Change the license if necessary (by default it's GNU GPL v3). Copy over the
``LICENSE`` file with the text of your license and edit the license field in
``pyproject.toml``. Find a `list of licenses here
<https://www.gnu.org/licenses/license-list.en.html>`_ and the `notation used by
Poetry here <https://python-poetry.org/docs/pyproject/#license>`_. Commit
the changes. E.g. if you want *MIT* license, copy `this text
<https://spdx.org/licenses/MIT.html>`_ to the ``LICENSE`` file and do the
changes below:

.. code:: bash

   find . -depth -type f -exec sed -i '' 's/GPLv3/MIT/g' {} +
   sed -i '' 's/GPL-3\.0-only/MIT/g' pyproject.toml
   git add LICENSE
   git add pyproject.toml
   git commit -nm 'set license to MIT'

Setup the tools
---------------

Initialize ``poetry`` and ``tox``:

.. code:: bash

   poetry update
   poetry install
   tox
   git add -u
   git commit -nm 'updated poetry lock'

Edit the readme. If you prefer markdown over rST, replace it by a markdown
file and change the ``readme`` field under the ``tool.poetry`` section of
``pyproject.toml``. Commit the changes.

Initialize ``pre-commit``. So far we run all commits with the ``-n`` switch
to disable hooks. If you skip this switch at your next commit, pre-commit
will come into action, install all the tools listed in
``.pre-commit-config.yaml``, and run them according to the settings.

.. code:: bash

   pre-commit install

Choose your code formatter
--------------------------

In the config there are three code formatter set up but all disabled. These
are YAPF, Black and fixit. To enable one of them, remove the
``stages: [manual]`` from its hook. In this case the code formatter will run
and change your files upon each commit. If you prefer to run it only manually,
you can do it by the command below (in this example YAPF):

.. code:: bash

   pre-commit run yapf --hook-stage manual

Set up your linter
------------------

In the ``tool.flake8`` section of ``pyproject.toml``,
add the codes of general or directory or file specific exceptions. In
code files for individual cases use the ``# noqa:`` tags.

Docs with Sphinx
----------------

A Github action is set up to build and publish your documentation on Github.
Edit ``docs/src/index.rst``, the main page of your documentation. You can
decide to leave the current readme included or write a completely different
document in ``docs/src/index.rst``.

Usage
=====

Once you finished the setup above, you can start developing your project.
You can read more about the usage of each tool on their webpages. See below
a handful of the most important tasks:

Do a commit without running pre-commit hooks
--------------------------------------------

Use the ``-n`` switch:

.. code:: bash

   git commit -nm 'commit message...'

Run the tests
-------------

With ``tox`` you can run the tests in an automatized way, potentially in
multiple environments. Calling ``tox`` runs everything that you set up in
``tox.ini``.

.. code:: bash

   tox

To run the tests directly via ``pytest``, simply do:

.. code:: bash

   poetry run pytest -v

Add a new dependency
--------------------

First add the new third party dependency to the ``tool.poetry.dependencies``
section of ``pyproject.toml``, by default with the ``"*"`` version
specification. Then let Poetry update the lock file and the virtual
environment. Finally, commit these changes.

.. code:: bash

   poetry update
   poetry install
   git add -u
   git commit -nm 'new dependency: some-package'

Build the docs
--------------

The docs are build automatically by the Github action after each push. To
build them also locally and manually:

.. code:: bash

   poetry run make html --directory docs/

Why should I run everything by ``poetry run``?
----------------------------------------------

Poetry maintains a virtual environment for your project. By running commands
with ``poetry run ...``, you run them in this virtual environment, where all
the dependencies are installed, as defined in ``poetry.lock``, along with the
latest version of your project.
