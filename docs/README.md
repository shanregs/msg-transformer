# Documentation

To run the app, start with running-locally.md. To understand it, read in the order below.

| Document | Read it to learn |
| --- | --- |
| [running-locally.md](running-locally.md) | **Start here to run it.** Build, run modes (rest, kafka, both, file, publisher), the message publisher, testing with curl, Postman or IntelliJ, running and debugging in IntelliJ, test cases with expected results, troubleshooting |
| [design-and-architecture.md](design-and-architecture.md) | What the system does, its design principles, the architecture, the key decisions and why, threads, failure handling, delivery guarantees, configuration |
| [code-flow-and-classes.md](code-flow-and-classes.md) | The flow of one message from entry point to CSV, the startup flow, write modes, daily files, offsets, every class, a worked example |
| [rules-compilation.md](rules-compilation.md) | How a JSON mapping rule becomes running code, every rule type, constants, startup validation, error messages, how to add a rule |
| [c4-architecture.md](c4-architecture.md) | C4 diagrams: context, containers, components, code, and sequences |

Background on how the Camel route and the write modes were designed: [design-camel-batch-writer.md](design-camel-batch-writer.md) (decisions, spike results, plan) and [c4-camel-batch-writer.md](c4-camel-batch-writer.md).

Diagrams are Mermaid and render on GitHub and in most IDEs.
