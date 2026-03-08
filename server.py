from flask import Flask, jsonify, request
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from orchestrator import OrchestratorService

app = Flask(__name__)
orchestrator_service = OrchestratorService()


@app.route('/api/ping')
def home():
    print('Server pinged')
    return "Server running"


@app.route('/api/talk-to-data', methods=['POST'])
def talk_to_data():
    """
    Endpoint for talking to data. Calls the orchestration service.

    Request body:
    {
        "question": "string",
        "data": {},
        "ontology_url": "url",
        "sessionId": "optional string"
    }
    """
    request_data = request.get_json()

    if not request_data:
        return jsonify({"error": "Request body is required", "status": "error"}), 400

    question = request_data.get("question")
    data_payload = request_data.get("data", {})
    ontology_url = request_data.get("ontology_url")
    session_id = request_data.get("sessionId")

    if not question:
        return jsonify({"error": "Question is required", "status": "error"}), 400
    if ontology_url is None:
        return jsonify({"error": "Ontology URL is required", "status": "error"}), 400

    try:
        result = orchestrator_service.talk_to_data(
            question, data_payload, ontology_url, session_id=session_id
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": "An internal error occurred.", "status": "error"}), 500


# ── Privacy budget endpoints ───────────────────────────────────────────

@app.route('/api/privacy-budget', methods=['GET'])
def get_privacy_budget():
    """Return the current privacy budget status.

    The budget is global (shared across all sessions).
    """
    return jsonify({
        "remaining_budget": orchestrator_service.budget_service.get_remaining(),
        "epsilon_total": orchestrator_service._config.epsilon_total,
        "epsilon_base": orchestrator_service._config.epsilon_base,
    })


@app.route('/api/privacy-budget/reset', methods=['POST'])
def reset_privacy_budget():
    """Reset the privacy budget (for testing / demo restarts only)."""
    orchestrator_service.budget_service.reset()
    return jsonify({
        "status": "success",
        "message": "Privacy budget reset.",
        "remaining_budget": orchestrator_service.budget_service.get_remaining(),
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
