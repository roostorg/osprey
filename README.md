# ROOST - Osprey

<img width="200" height="64" alt="Copy of ROOST-Mark-Yellow" src="/images/ROOST-Horizontal-Yellow.png" />

# Welcome to Osprey

<img alt="Osprey UI Sample" src="./images/query-and-charts.png" />


## Overview

This repository is home to one of ROOST’s safety tools, the Osprey rules engine. Osprey is an open-source event stream decisions engine and analysis UI designed to investigate and take automatic action on events and their properties as they happen in real-time. Originally developed internally at [Discord](https://discord.com/) to combat spam, abuse, botting, and scripting across its platform, Osprey has now been open-sourced to help other platforms facing similar challenges.

Osprey is a library for processing actions through human-written rules and outputting verdicts & custom effects back to configurable output sinks. It evaluates events using structured rule logic (SML) that is extendable via user-defined functions (UDFs). Osprey can also track state across events by labelling entities if implementers provide a labels service backend (see [labels_service.py](./example_plugins/src/services/labels_service.py) for a Postgres-backed labels service example)

This 'Rules \+ Investigation' tool is able to:

- take action based on user behavior
- combine actions with human written rules
- let operators query human actions and past decisions
- perform investigations or write new rules based on decisions

## v0 Release Notes
The v0 release is built for engineers and Trust & Safety teams who want to explore, test, and integrate Osprey's core capabilities into their platform for incident response and Trust & Safety investigation.

Some UI features have been planned for the v1 release to ensure a solid foundation for experimentation:

- **Bulk actions** - Mass investigation and response operations through the UI
- **Labels** - Visual entity tagging and categorization systems in the interface
- **Event stream in UI** - Real-time event monitoring and alert dashboard
- **Load Balancing** - Better performance efficiency and management of sync/async rules

### Feedback Wanted
This is a working system, not a prototype. Try it locally, connect your data, write some rules, and tell us what's missing for your use case. We're particularly interested in:

- Integration challenges with your existing platform infrastructure
- Performance characteristics with your event volumes and rule complexity
- Missing detection capabilities or response actions you need
- API improvements that would make adoption easier for your team

Your experimentation feedback will directly shape v1 priorities and help us build the most useful Trust & Safety tooling for the community.

## Join Us
Writing code is not the only way to help the project. Reviewing pull requests, answering questions to help others on mailing lists or issues, providing feedback from a domain expert perspective, organizing and teaching tutorials, working on the website, improving the documentation, are all priceless contributions.

- Join us on [Discord server](https://discord.gg/5Csqnw2FSQ)
- Join our [newsletter](https://roost.tools/#get-started) for more announcements and information
- Follow us on [Bluesky](https://bsky.app/profile/roost.tools) or [LinkedIn](https://www.linkedin.com/company/roost-tools/)

_ROOST (Robust Open Online Safety Tools), a non-profit organization that brings together expertise, resources, and investments from major technology companies and philanthropies to build scalable, interoperable safety infrastructure for the AI era._

## 🚀 Quick Start

### Prerequisites

- Python 3.11 or higher
- Git
- [uv](https://docs.astral.sh/uv/) (Python package manager) or python package manager of choice

### Installation

1. **Clone the repository:**

   ```bash
   git clone git@github.com:roostorg/osprey.git
   cd osprey
   ```

2. **Install dependencies using uv:**

   ```bash
   uv sync
   ```

3. **Set up development tools:**

   ```bash
   uv run pre-commit install
   ```

4. **Verify setup:**

   ```bash
   # Test linting
   uv run ruff check
   uv run mypy .

   # Test formatting
   uv run ruff format --diff

   # Test pre-commit hooks
   uv run pre-commit run --all-files

5. **Start Services:**

   ```bash
   docker compose up -d
   ```

   or using the wrapper script

   ```bash
   ./start.sh
   ```

   this starts the osprey-worker on its own along with all its required dependencies.

   alternatively, you can start Osprey with `osprey-coordinator`, refer to the [Coordinator README](./example_docker_compose/run_osprey_with_coordinator/README.md) for more information

6. (Optional) **Port Forward the UI/UI API:**

   If you are running the docker compose on a headless machine, you will need to port forward the UI and UI API.
   Namely, ports `5002` (UI) and `5004` (UI API). Then, you can connect via http://localhost:5002/ :D


### Development Workflow

- See [DEVELOPMENT.md](./docs/DEVELOPMENT.md) for comprehensive development setup and workflow documentation
- All code changes should pass linting (Ruff) and type checking (MyPy)
- Pre-commit hooks automatically run on each commit to maintain code quality


## Recognition
Discord uses Osprey to quickly detect and remove new types of harm that put users at risk. Rather than leaving other platforms to build similar tools from scratch, ROOST and Discord have open-sourced this powerful rule engine in collaboration with [internet.dev](https://internet.dev/) to make it available for anyone who needs it.
