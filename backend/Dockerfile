FROM node:18-alpine

WORKDIR /app

COPY package*.json ./

# Changed from 'npm ci' to 'npm install' because package-lock.json might be missing
RUN npm install --only=production

COPY server.js ./

EXPOSE 8080

CMD ["node", "server.js"]