---
name: commit-changes
description: Create git commits that match Opexa conventions. Use when the user asks to commit changes (including "commit changes" or "commit all changes").
license: MIT
compatibility: opencode
---

## When to Use

- User asks to commit changes
- User says "commit changes" or similar
- User invokes the commit-changes skill, e.g. /commit-changes

## Execution Behavior

- **Always use the `read` agent first** to inspect `git status` and `git diff` (staged and unstaged). Never assume the working tree is clean without checking.
- **Use the `read` agent to summarize the uncommitted changes and draft the commit message** before staging files or creating commits.
- Proceed immediately with commit workflow when invoked
- Default to committing all current changes when no file list is provided
- If changes span multiple areas or scopes, always split them into focused commits without asking for confirmation
- Only ask a question if changes include sensitive files or the intended commit scope is genuinely ambiguous

## Workflow

1. **Inspect uncommitted changes with the `read` agent.** Have the `read` agent review the current worktree and return:

   - A concise summary of the uncommitted changes grouped by area or file.
   - The likely purpose of the changes.
   - A proposed commit scope split when the changes span multiple areas.
   - A draft commit message for each proposed commit.

2. **Decide commit scope.** Use the `read` agent output to determine whether the changes belong in one commit or multiple commits. When the changes span multiple areas or scopes, split them into focused commits automatically.

3. **Create the commit.** Stage the relevant files and use the drafted commit message, adjusting it only if needed to match the final staged scope.

## Commit Message Format

```
<type>: <summary>

- <detail 1>
- <detail 2>
- <detail 3>
- ...
# or
<type>(<scope>): <summary>

- <detail 1>
- <detail 2>
- <detail 3>
- ...
```

### Examples

**Feature commits:**

- `feat(wallet): add gcash direct webpay deposit support`
- `feat(web-admin): add title & content field on reject member verification mutation`
- `feat(account): implement member account suspension workflow`

**Fix commits:**

- `fix(game): resolve race condition in game round settlement`
- `fix(report): correct decimal precision in revenue calculations`

**Chore/DevOps commits:**

- `chore(wallet): add script to cancel stuck pending aio deposits`
- `chore(packages): bump arque to v2.3.0`

---

## Commit Types

| Type       | Description                                     |
| ---------- | ----------------------------------------------- |
| `feat`     | New features or enhancements                    |
| `fix`      | Bug fixes                                       |
| `docs`     | Documentation changes only                      |
| `test`     | Adding or updating tests                        |
| `chore`    | Configuration, dependencies, or tooling changes |
| `devops`   | CI/CD, infrastructure, or deployment changes    |
| `security` | Security improvements or vulnerability fixes    |
| `refactor` | Code restructuring without behavior changes     |

---

## Scopes

Use the scope that best matches the primary area of the codebase affected. If the scope is unclear, omit it.

### Backend Services

- `account` - Account service (member management, profiles)
- `auth` - Authentication service (login, sessions, tokens)
- `wallet` - Wallet service (deposits, withdrawals, balances)
- `game` - Game service (rounds, bets, settlements)
- `report` - Report service (analytics, projections)
- `trigger` - Trigger service (automations, webhooks)
- `broker` - Broker service (event routing)
- `extension` - Extension service (third-party integrations)
- `migration` - Database migrations
- `nexusplay`
- `opexapay`

### Frontend Services

- `web-admin` - Admin console
- `dashboard` - Dashboard

### Other

- `packages` - Shared packages in `/packages`
- `opencode` - Opencode configuration
- `nx` - nx configuration
- `cron` - cron jobs

---

## Writing Guidelines

### Summary Line

- Use **imperative mood** (e.g., "add", "fix", "update", not "added", "fixes", "updated")
- Start with **lowercase** (Opexa convention)
- Keep it **under 72 characters**
- Be **specific** about what changed

✅ Good: `fix(wallet): prevent duplicate deposit processing`
✅ Good: `docs(opencode): refine commit examples`
✅ Good: `chore: trigger deployment for web-admin`
❌ Bad: `fix(wallet): fixed bug`

### Commit Body

- The commit body is **required** for all commits
- Add a bulleted list with **up to 10 entries** describing changes in detail
- Each entry should be a complete, **imperative** statement
- Add a blank line between the summary and the first detail item
- Do **not** insert blank lines between detail items; keep detail bullets consecutive
- Focus on **what** and **why**, not **how**

```
feat(account): add member suspension feature

- add SuspendMemberAccount command and event
- implement suspension validation in account aggregate
- add suspension status to member profile projection
- expose suspendMember mutation in GraphQL API
- add unit tests for suspension workflow
```

### Response Requirements

- Always show the final commit message at the end of your response

---

## Multi-Scope Changes

If changes span multiple scopes, **create separate focused commits** for each scope, and do it automatically without asking the user whether to proceed:

This improves:

- **Code review** - reviewers can focus on related changes
- **Reverts** - easier to revert specific changes if needed
- **History** - cleaner git log and blame

---

## Quick Reference

```bash
# Feature
git commit -m "feat(<scope>): <what you added or changed>"

# Bug fix
git commit -m "fix(<scope>): <what you fixed>"

# Documentation
git commit -m "docs(<scope>): <what you documented>"

# Tests
git commit -m "test(<scope>): <what you tested>"

# Chore (deps, config)
git commit -m "chore(<scope>): <what you configured>"

# DevOps (CI/CD, infra)
git commit -m "devops(<scope>): <what you deployed/configured>"

# Security
git commit -m "security(<scope>): <what you secured>"
```
