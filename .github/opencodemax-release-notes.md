OpenCodeMAX is a fork of OpenCode focused on higher-agentic-performance terminal coding.

This fork adds:

- `/goal` for long-running autonomous objectives
- `/btw` for inline side-thread conversations
- Local parallel subagent swarms
- Team task boards and worker coordination tools
- Local-first background workers that can do generic implementation work, not just research
- Improved swarm permission behavior so YOLO/autonomous workers are not forced through master approval for every edit

Install:

```bash
curl -fsSL https://github.com/patrick-toulme/opencode/releases/latest/download/install.sh | bash
```

The CLI binary inside these archives is still named `opencode` for compatibility with upstream internals.
