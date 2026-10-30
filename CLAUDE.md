# OpenWolf

@.wolf/OPENWOLF.md

This project uses OpenWolf for context management. Read and follow .wolf/OPENWOLF.md every session. Check .wolf/cerebrum.md before generating code. Check .wolf/anatomy.md before reading files.
## Gotchas & Pitfalls

- **Always use venv Python**: `python`/`pytest` directly fail — use `.venv/Scripts/python.exe` (Windows)
- **NiceGUI global CSS**: `ui.add_head_html()` outside a `@ui.page` handler requires `shared=True`; else RuntimeError at startup
- **PyInstaller entry**: `app/main.py` must call `main()` under `if __name__ == "__main__": multiprocessing.freeze_support(); main()`
- **PyInstaller spec paths**: All paths in `finacialsim.spec` must use `str(project_root / "...")` — bare relative paths resolve to spec dir, not project root
- **Alembic in frozen EXE**: Use Alembic Python API (`Config` + `alembic_command.upgrade`), never `subprocess [sys.executable, "-m", "alembic"]`
- **SQLite + Windows tests**: Call `engine.dispose()` after session close in fixtures to release the connection pool before temp dir cleanup

## Core Principles

- **Simplicity First**: Every change as simple as possible. Minimal code impact.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Minimal Impact**: Touch only what's necessary. No new bugs.

## Task Management

**Plan First**:

- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions).
- Write plan to `tasks/todo.md` with checkable items and current date.
- Goes sideways: STOP, re-plan immediately. No pushing.
- Write detailed specs upfront. Reduce ambiguity.

**Verify Plan**:

- Plan mode for verification, not just building.
- Check in before starting implementation.

**Track Progress**:

- Mark items complete as you go, recording date and time.

**Explain Changes**:

- High-level summary at each step.

**Document Results**:

- Add review section to `tasks/todo.md`.

**Capture Lessons**:

- Update `tasks/lessons.md` after corrections.

---

## Subagent Strategy

- Use subagents liberally. Keep main context clean.
- Offload research, exploration, parallel analysis to subagents.
- Complex problems: throw more compute via subagents.
- One task per subagent.

---

## Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

---

## Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

---

## Self-Improvement Loop

- After ANY correction from user: update `tasks/lessons.md` with pattern.
- Write rules to prevent same mistake.
- Iterate ruthlessly until mistake rate drops.
- Review lessons at session start.

---

## Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

---

## Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

```text
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

## Verification Before Done

- Never mark task complete without proof it works.
- Diff behavior main vs changes when relevant.
- Ask: "Would staff engineer approve this?"
- Run tests, check logs, demonstrate correctness.

---

## Demand Elegance (Balanced)

- Non-trivial changes: pause. Ask "is there more elegant way?"
- Fix feels hacky: "Knowing everything I know now, implement elegant solution."
- Skip for simple, obvious fixes. No over-engineering.
- Challenge own work before presenting.

---

## Autonomous Bug Fixing

- Bug report: fix it. No hand-holding.
- Point at logs, errors, failing tests. Resolve them.
- Zero context switching for user.
- Go fix failing CI tests without being told how.

---

## Self-Maintenance Rule

After every major change (new model, new page, new controller, route changes, migration changes, new test files, architectural shifts), update this CLAUDE.md file to reflect the current state. Specifically:

- Update the Project Structure section if directories or key files change
- Add new gotchas or patterns to the "Gotchas & Pitfalls" section
- Keep this file as the single source of truth for AI sessions working on this project

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

## Agent skills

### Issue tracker

Issues live in GitHub Issues for this repo. See `docs/agents/issue-tracker.md`.