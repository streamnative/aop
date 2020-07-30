# Contributing

The AOP community welcomes contributions from anyone with a passion for AMQP on Pulasr! 

We look forward to working with you!

### One-time Setup

#### Obtain a GitHub account

We use GitHub’s pull request functionality to review proposed code changes.

If you do not already have a personal GitHub account, sign up [here](https://github.com/join).

#### Fork the repository on GitHub

Go to the [AOP GitHub Repo](https://github.com/streamnative/aop) and fork the repository
to your own private account. This will be your private workspace for staging changes.

#### Clone the repository locally

You are now ready to create the development environment on your local machine. Feel free to repeat
these steps on all machines that you want to use for development.

We assume you are using SSH-based authentication with GitHub. If necessary, exchange SSH keys with
GitHub by following [their instructions](https://help.github.com/articles/generating-an-ssh-key/).

Clone your personal AOP’s GitHub fork.

    $ git clone https://github.com/<Github_user>/aop.git
    $ cd aop
  
 Add Streamnative Repo as additional Git remotes, where you can sync the changes (for committers, you need
 these two remotes for pushing changes).
 
 	$ git remote add streamnative https://github.com/streamnative/aop


You are now ready to start developing!

###### Initial setup

1. Import the Pulsar projects

	File
	-> Import...
	-> Existing Maven Projects
	-> Browse to the directory you cloned into and select "aop"
	-> make sure all aop projects are selected
	-> Finalize

You now should have all the AOP projects imported into eclipse and should see no compile errors.

### Create a branch in your fork

You’ll work on your contribution in a branch in your own (forked) repository. Create a local branch, initialized with the state of the branch you expect your changes to be merged into. Keep in mind that we use several branches, including `master`, feature-specific, and release-specific branches. If you are unsure, initialize with the state of the `master` branch.

	$ git fetch streamnative
	$ git checkout -b <my-branch> streamnative/master

At this point, you can start making and committing changes to this branch in a standard way.

### Syncing and pushing your branch

Periodically while you work, and certainly before submitting a pull request, you should update your branch with the most recent changes to the target branch.

    $ git pull --rebase

Remember to always use `--rebase` parameter to avoid extraneous merge commits.

Then you can push your local, committed changes to your (forked) repository on GitHub. Since rebase may change that branch's history, you may need to force push. You'll run:

	$ git push origin <my-branch> --force

### Testing

All code should have appropriate unit testing coverage. New code should have new tests in the same contribution. Bug fixes should include a regression test to prevent the issue from reoccurring.

### Licensing

All code contributed to AOP will be licensed under [Apache License V2](https://www.apache.org/licenses/LICENSE-2.0). You need to ensure every new files you are adding have the right
license header. You can add license header to your files by running following command:

```shell
$ mvn license:format
```

## Review

Once the initial code is complete and the tests pass, it’s time to start the code review process. We review and discuss all code, no matter who authors it. It’s a great way to build community, since you can learn from other developers, and they become familiar with your contribution. It also builds a strong project by encouraging a high quality bar and keeping code consistent throughout the project.

### Create a pull request

Organize your commits to make a committer’s job easier when reviewing. Committers normally prefer multiple small pull requests, instead of a single large pull request. Within a pull request, a relatively small number of commits that break the problem into logical steps is preferred. For most pull requests, you'll squash your changes down to 1 commit. You can use the following command to re-order, squash, edit, or change description of individual commits.

    $ git rebase -i streamnative/master

You'll then push to your branch on GitHub. Note: when updating your commit after pull request feedback and use squash to get back to one commit, you will need to do a force submit to the branch on your repo.

Navigate to the [AOP GitHub Repo](https://github.com/streamnative/aop) to create a pull request.

In the pull request description, please include:

 * Motivation : Why is this change needed? What problem is addressing?
 * Changes: Summary of what this pull request is changing, to help reviewers at better understanding
   the changes.

Please include a descriptive pull request message to help make the comitter’s job easier when reviewing.
It’s fine to refer to existing design docs or the contents of the associated issue as appropriate.

If the pull request is fixing an issue, include a mention to in the description, like:

```
Fixes #1234
```

This will automatically change the state on the referenced issues.

If you know a good committer to review your pull request, please make a comment like the following. If not, don’t worry -- a committer will pick it up.

	Hi @<GitHub-committer-username>, can you please take a look?

When choosing a committer to review, think about who is the expert on the relevant code, who the stakeholders are for this change, and who else would benefit from becoming familiar with the code. If you’d appreciate comments from additional folks but already have a main committer, you can explicitly cc them using `@<GitHub-committer-username>`.

### Code Review and Revision

During the code review process, don’t rebase your branch or otherwise modify published commits, since this can remove existing comment history and be confusing to the committer when reviewing. When you make a revision, always push it in a new commit.

Our GitHub repo automatically provides pre-commit testing coverage using Github action. Please make sure those tests pass; the contribution cannot be merged otherwise.

### LGTM

Once the committer is happy with the change, they’ll approve the pull request with an LGTM (“*looks good to me!*”) or a `+1`. At this point, the committer will take over, possibly make some additional touch ups, and merge your changes into the codebase.

In the case the author is also a committer, either can merge the pull request. Just be sure to communicate clearly whose responsibility it is in this particular case.

Thank you for your contribution to Pulsar!

### Deleting your branch
Once the pull request is merged into the Pulsar repository, you can safely delete the branch locally and purge it from your forked repository.

From another local branch, run:

	$ git fetch origin
	$ git branch -d <my-branch>
	$ git push origin --delete <my-branch>
