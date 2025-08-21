# Builder stage
FROM python:3.11 AS builder

WORKDIR /app

# Copy all required files for Poetry installation
COPY pyproject.toml poetry.lock README.md ./
# Copy the app directory if your code is structured that way
COPY app/ ./app/

# Install Poetry
RUN pip install --no-cache-dir poetry

# Configure Poetry to create virtualenv in project directory
RUN poetry config virtualenvs.in-project true

# Install dependencies (including the project)
RUN poetry install --no-cache

# Release stage
FROM python:3.11 AS release
WORKDIR /app

# Copy the entire app directory (including .venv) from builder
COPY --from=builder /app /app
COPY workers/ /app/workers/
COPY app.db /app/app.db
# No need for additional COPY . /app if builder stage has everything
# COPY . /app  # Comment this out to avoid overwriting .venv

# Set the virtualenv path
ENV PATH="/app/.venv/bin:$PATH"

# Expose the application port
EXPOSE 8333

# Command to run the FastAPI app with 
CMD ["/app/prestart.sh"]
# CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8333", "--proxy-headers"]