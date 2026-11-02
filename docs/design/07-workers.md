# 07 – Workers

A worker carries out one task inside one workspace branch. The contract is deliberately narrow:

```
run_task(worker, repo, task)  →  fork (or resume) branch ws/<task.id> from main
                                 worker.run(task, repo, agent_id)
                                 result: status, summary, commit, touched files, steps, tokens
```

The worker commits to its own branch and never to main. Landing a branch is the merge queue's job,
which keeps "did the agent finish" separate from "may this change enter main".

## LoopWorker

A plain tool-calling loop written against `ChatModel.chat(messages, tools) -> Reply`.
`OpenAICompatModel` implements it for the chat-completions protocol (Ollama, llama.cpp, vLLM, hosted
APIs); the URL and model name are the only configuration. `ScriptedModel` plays back prepared replies
for tests, so the loop is verified without any model.

Design choices aimed at weak local models:

- six tools (`list_files`, `read_file`, `write_file`, `replace_in_file`, `delete_file`, `move_file`) plus
  `finish`. `replace_in_file` requires a unique match, so a model that misremembers a file gets an error
  instead of a silent wrong edit.
- every tool failure is returned as `error: ...` text the model can act on; nothing raises into the loop.
  Arguments that are not valid JSON are reported back the same way.
- a reply with no tool call is nudged twice ("use the tools, call finish"), then the worker gives up.
- a step limit bounds cost. Only a run that ends in `finish` commits; `gave_up`, `step_limit` and
  `error` leave the branch exactly at the fork point.

The loop works on the in-memory workspace overlay, so its reads are recorded for stale-read detection at
submit time, and `touched` (every path it wrote, deleted or moved) is available for scope checks (4B).

## CliWorker

Materializes the branch as a folder (`Checkout.clone`), runs an existing command-line agent there
(`{prompt}` in argv is replaced by the task text, otherwise the prompt goes to stdin) and pushes what it
left. Non-zero exit, timeout or a missing binary leave the branch untouched. This is how an existing tool such
as `claude -p` joins the same branches and merge path as the built-in loop.

## Limits

- the loop cannot run programs; tests are run by the merge queue on the merged result (4C), not by the agent.
- token counts come from the endpoint's `usage` field and are 0 when a server omits it.
- no streaming, no parallel tool-call limit, no context-window management: long tasks should be split by
  the planner (4B), not stretched.
