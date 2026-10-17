# Documentation

To run the app, start with running-locally.md. To understand it, read in the order below.

| Document | Read it to learn |
| --- | --- |
| [running-locally.md](running-locally.md) | **Start here to run it.** Database setup (Postgres), build, running a table run, Swagger UI, testing single messages, running and debugging in IntelliJ, test cases with expected results, troubleshooting |
| [design-and-architecture.md](design-and-architecture.md) | What the system does, its design principles, the architecture, the key decisions and why, threads, failure handling, re-runs, configuration |
| [code-flow-and-classes.md](code-flow-and-classes.md) | The startup flow, a run over the table, the flow of one record to the CSV, write modes, daily files, every class, a worked example |
| [rules-compilation.md](rules-compilation.md) | How a JSON mapping rule becomes running code, every rule type, constants, startup validation, error messages, how to add a rule |
| [c4-architecture.md](c4-architecture.md) | C4 diagrams: context, containers, components, code, and sequences |

SQL scripts to create the local database and the sample trade table are in `samples/db`.

Diagrams are Mermaid and render on GitHub and in most IDEs.
