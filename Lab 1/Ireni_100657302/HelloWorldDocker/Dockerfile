# Use the OpenJDK 14 image as the base image
FROM openjdk:14

# Create a new app directory for my app files
RUN mkdir /app

# Copy the app files from host machine to image filesystem
COPY out/production/HelloWorldDocker/ /app

# Set directory for executing future commands
WORKDIR /app

# Run the main class
CMD java Main