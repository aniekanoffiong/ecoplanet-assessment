FROM node:23-slim
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN cp .env.example .env
EXPOSE 3000
CMD ["npm", "start"]
