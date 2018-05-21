#!/usr/bin/env bash
#
# A git pre-commit hook to format the go files added
# and make sure no files with invalid names are committed.
#
# To enable this hook, add a symbolic link to this file in your
# repository's .git/hooks directory with the name "pre-commit"

exec 1>&2

# The below check to make sure file names don't include unusual characters was
# modified from the example pre-commit hook found in any recently created repo
NEW_NAMES=$(git diff --cached --name-only --diff-filter=ACR -z)
if echo "${NEW_NAMES}" | grep -q [^a-zA-Z0-9\.\_\\\/\-]; then
  echo "Error: Attempt to add an invalid file name"
  echo "${NEW_NAMES}" | grep [^a-zA-Z0-9\.\_\\\/\-]
  echo
  echo "File names can only include letters, numbers,"
  echo "hyphens, underscores and periods"
  echo
  echo "Please rename your file before committing it"
  exit 1
fi


# Now make sure all of the go source code is properly formatted by gofmt
FAILED=''
CHANGED_GO=$(git diff --cached --name-only --diff-filter=ACRM | grep '\.go$')
# make sure we have something
if [ -n "${CHANGED_GO}" ]; then
  # I've had some trouble with conflicts when popping stashes after formatting
  # and I am not willing to compromise my ability to stage only some changes
  # in a file so we will simply fail if things aren't formatted instead of
  # trying to stage formatted code in here

  old_stash=$(git rev-parse -q --verify refs/stash)
  git stash save --include-untracked --keep-index --quiet "pre-commit"
  new_stash=$(git rev-parse -q --verify refs/stash)

  # find any of the staged files that differ from what gofmt produces
  UNFORMATTED=$(gofmt -l ${CHANGED_GO}) || FAILED='true'

  # http://stackoverflow.com/questions/20479794/how-do-i-properly-git-stash-pop-in-pre-commit-hooks-to-get-a-clean-working-tree
  # see above link for explaination of the check on stash states and the reset
  if [ "$old_stash" != "$new_stash" ]; then
    git reset --hard -q
    git stash pop --quiet --index
  fi

  # go ahead and format everything now that we don't have a stash to conflict with
  gofmt -w ${CHANGED_GO}
fi

if [ -n "${FAILED}" ]; then
  echo "gofmt failed"
  echo "$UNFORMATTED"
  exit 1
fi

if [ -n "${UNFORMATTED}" ]; then
  echo "impropertly formatted GO files"
  echo "$UNFORMATTED"
  exit 1
fi

exit 0
