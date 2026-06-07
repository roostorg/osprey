FROM node:18-alpine

WORKDIR /app

COPY osprey_ui/package.json osprey_ui/pnpm-lock.yaml osprey_ui/.npmrc ./

RUN npm install -g corepack@0.33.0 && corepack enable

RUN pnpm install --frozen-lockfile

COPY osprey_ui/ .

EXPOSE 5002

CMD ["pnpm", "start"]
