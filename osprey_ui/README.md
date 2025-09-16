# Osprey UI

### Prerequisites

- Node.js (version 16+ recommended)
- npm package manager
- Docker (for backend services)

## Purpose

`osprey_ui` is the frontend repository for the Osprey application.
For more information on the application as a whole, its features,
and what it is used for, please refer to the [documentation](https://github.com/roostorg/osprey/tree/main/docs). This is a `create-react-app` project. It uses React/Typescript/Zustand,
[Ant Design 4.0](https://ant.design/) as a UI framework, and CSS Modules.

### Service Dependencies

- **Osprey UI API** - Main backend API
- **Osprey Worker** - Rules processing engine
- **Kafka** - Message streaming
- **PostgreSQL** - Database
- **Druid** - Analytics engine

## Development Setup

1. **Start Backend Services**

```bash
# From the osprey project root directory
docker compose up -d
```

This starts all required backend services including Kafka, PostgreSQL, Druid, and the Osprey API.

2. **Install Dependencies**

```bash
cd osprey_ui
npm install
```

3. **Start Development Server**

```bash
npm start
```

The UI will be available at **http://localhost:5002** unless otherwise specified, and will automatically connect to the backend services running in Docker containers.

## Deploy

TODO: TBD
