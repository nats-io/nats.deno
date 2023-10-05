FROM denoland/deno:1.29.1

# Set the working directory in the container to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app
ENV NATS_URL=localhost:4222
# Run the specified deno command
CMD ["run", "--allow-env", "--allow-net", "./bin/os_compatibility.ts"]
