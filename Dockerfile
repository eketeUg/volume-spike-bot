# --- Stage 1: Build Stage ---
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package manifests
COPY package*.json ./

# Install dependencies
RUN npm ci

# Copy source files
COPY . .

# Build application
RUN npm run build

# --- Stage 2: Production Stage ---
FROM node:20-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production

# Copy package manifests & install production dependencies only
COPY package*.json ./
RUN npm ci --only=production

# Copy compiled JavaScript build from builder stage
COPY --from=builder /app/dist ./dist

# Default port
EXPOSE 3000

CMD ["node", "dist/main.js"]
