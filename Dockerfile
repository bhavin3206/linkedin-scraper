# Use an official Python image as the base
FROM python:3.10

# Set the working directory inside the container
WORKDIR /app

# Copy the project files into the container
COPY . /app

# Install required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Set display environment for Selenium (if needed)
ENV DISPLAY=:99

# Command to run the scraper
CMD ["python", "main.py"]
