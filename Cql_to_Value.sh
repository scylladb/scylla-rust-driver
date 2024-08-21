#!/bin/bash

# Script to rename T to U in the current commit
rename_T_to_U() {
    git grep -l 'DeserializeValue' | xargs sed -i 's/\bDeserializeValue\b/DeserializeValue/g'
    git add -u
    git rebase --continue
}

# Start interactive rebase
    git rebase -i bd80a54b

# Interactive rebase will stop at each commit we marked as 'edit'
while [ $? -eq 0 ]; do
    # Apply renames
    rename_T_to_U

    # Continue rebase
    git rebase --continue
done
