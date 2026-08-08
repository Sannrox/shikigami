# Offline evaluation fixtures

shikigami eval runs deterministic scripted model cases through the real
Harness and checks structured outcomes plus workspace files. It is intended
for regression fixtures and adapter changes; it does not contact a model
provider or sekai-chisei.

~~~
shikigami eval examples/eval-smoke.json
shikigami eval examples/eval-smoke.json --json
~~~

Fixture schema version is 1:

~~~
{
  "schema_version": 1,
  "name": "coding-smoke",
  "cases": [
    {
      "name": "writes marker",
      "task": "write a marker",
      "script": [
        {
          "tool_calls": [
            {
              "name": "write_file",
              "args_json": "{\"path\":\"ok.txt\",\"content\":\"hello\"}"
            }
          ]
        },
        {
          "tool_calls": [
            {
              "name": "report",
              "args_json": "{\"summary\":\"done\",\"success\":true}"
            }
          ]
        }
      ],
      "expect_success": true,
      "summary_contains": ["done"],
      "expect_files": [{"path": "ok.txt", "contains": "hello"}]
    }
  ]
}
~~~

Each case gets an isolated temporary state/workspace and is cleaned after the
suite. Expected paths are relative and cannot traverse outside the workspace.
The JSON result records pass/fail, run id, summary, and assertion failures.
