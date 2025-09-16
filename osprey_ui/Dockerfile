FROM node:18-alpine

WORKDIR /app

COPY osprey_ui/package.json osprey_ui/package-lock.json* ./

RUN npm install --legacy-peer-deps

COPY osprey_ui/ .

EXPOSE 5002

CMD ["npm", "start"]
