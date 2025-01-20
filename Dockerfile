# Use an official Python runtime as a base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the entire Data_Bazaar directory into the container
COPY ./ /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Specify the command to run your script
CMD ["python", "webscrapers/yfinance_data.py"]
