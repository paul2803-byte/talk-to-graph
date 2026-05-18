# Talk to Data Service

This service provides an API for querying and interacting with data using Large Language Models (LLMs) while applying differential privacy.

## Getting Started

Follow these steps to set up and run the service locally.

### 1. Prerequisites

- Python 3.8+ 

### 2. Install Dependencies

It is recommended to use a virtual environment. Install the required dependencies using `pip`:

```bash
pip install -r requirements.txt
```

### 3. Environment Variables

Before starting the server, you need to configure your environment variables. Copy the `.env.example` file to a new file named `.env`:

```bash
cp .env.example .env
```

Set the appropriate values in your `.env` file. The service supports the following environment variables:

**LLM Configuration:**
- `LLM_PROVIDER`: The LLM provider to use. Supported options: `openai`, `anthropic`, `azure`, `google`.
- `LLM_API_KEY`: Your API key for the chosen LLM provider.
- `LLM_MODEL`: The specific model to use (e.g., `gpt-4o`, `claude-3-opus-20240229`, `gemini-1.5-flash`).
- `RESPONSE_LLM_PROVIDER` (Optional): Override the provider used specifically for generating the final response.
- `RESPONSE_LLM_MODEL` (Optional): Override the model used specifically for generating the final response.

**Privacy Configuration:**
- `EPSILON_TOTAL`: Total privacy budget available.
- `EPSILON_BASE`: Base privacy budget consumption.
- `MIN_GROUP_SIZE`: Minimum group size for data aggregation to preserve privacy.

**Other:**
- `LOG_LEVEL` (Optional): Logging level (e.g., `INFO`, `DEBUG`). Defaults to `INFO`.

### 4. Start the Server

Start the Flask server by running:

```bash
python server.py
```

The server will start and listen on `http://0.0.0.0:8000`.

## API Endpoints

The service exposes the following main endpoints:

### Talk to Data
- **URL**: `/api/talk-to-data`
- **Method**: `POST`
- **Description**: Main endpoint for asking questions about your data.
- **Payload Example**:
  ```json
  {
      "question": "Your question here",
      "data": {},
      "ontology_url": "https://example.com/ontology",
      "sessionId": "optional-session-id",
      "privacy_mode": true
  }
  ```

### Privacy Budget
- **URL**: `/api/privacy-budget`
- **Method**: `GET`
- **Description**: Retrieve the current remaining global privacy budget.

- **URL**: `/api/privacy-budget/reset`
- **Method**: `POST`
- **Description**: Reset the privacy budget (useful for testing/demo purposes).

### Health Check
- **URL**: `/api/ping`
- **Method**: `GET`
- **Description**: Simple ping endpoint to check if the server is running.

## Further Documentation

For more detailed information about the internal workings and the orchestration logic, please refer to the `SERVICE.md` and `api.md` files included in the repository.
