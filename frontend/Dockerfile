FROM node:18-slim

# Set working directory
WORKDIR /app

# Install necessary build tools and libraries
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy package.json and package-lock.json
COPY package*.json ./

# Install dependencies, including optional ones
RUN npm install --include=optional \
    && npm install axios \
    && npm rebuild lightningcss --build-from-source

# Copy the rest of the application code
COPY . .

# Expose the Next.js development port
EXPOSE 3000

# Start the development server
CMD ["npm", "run", "dev"]
