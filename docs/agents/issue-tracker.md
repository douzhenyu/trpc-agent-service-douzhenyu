# Issue tracker

This repository uses GitHub Issues as its issue tracker.

## Standard operations

Use the GitHub CLI (`gh`) for issue and pull request operations:

```bash
# List open issues
gh issue list --state open

# View an issue
gh issue view <number>

# Create an issue
gh issue create --title "..." --body "..."

# Add or remove labels
gh issue edit <number> --add-label "..."
gh issue edit <number> --remove-label "..."

# Create a pull request
gh pr create --title "..." --body "..."
```

## Agent workflow

1. Read the issue before changing code and use its acceptance criteria as the source of truth.
2. Keep issue updates concise and evidence-based: summarize the change and the verification performed.
3. Treat pull requests as an explicit request surface. Do not open a PR unless the user asks for one.
4. Before closing an issue, confirm that its acceptance criteria are met or document the remaining blocker.

## Navigation

If the task spans unfamiliar code, start from the repository's local instructions and domain documentation, then trace only the relevant code paths. Prefer narrow, verifiable changes over broad refactors.
