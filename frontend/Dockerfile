FROM node:18-alpine AS build

WORKDIR /app

COPY package*.json ./

# Changed from 'npm ci' to 'npm install' because package-lock.json might be missing
RUN npm install

COPY public/ ./public/
COPY src/ ./src/

RUN npm run build

FROM node:18-alpine

WORKDIR /app

RUN npm install -g serve

COPY --from=build /app/build ./build

EXPOSE 3000

CMD ["serve", "-s", "build", "-l", "3000"]