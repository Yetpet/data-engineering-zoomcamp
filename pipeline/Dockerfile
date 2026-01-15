#start with slim python image 3.13
FROM python:3.13.11-slim

#install dependencies
#RUN pip install pandas pyarrow

#Copy uv from offical astral uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:Latest /uv /bin/

#set working directory
WORKDIR /code

# Add virtual environment to PATH so we can use installesd packages
ENV PATH="/code/.venv/bin:$PATH"

#Copy dependency files first (better layer caching)
COPY pyproject.toml .python-version uv.lock ./

#install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

#Copy application code
COPY pipeline.py .

# #set entrypoint
# ENTRYPOINT ["uv", "run", "python", "pipeline.py"]

#set entrypoint
ENTRYPOINT ["python", "pipeline.py"]