from flask import Flask, jsonify, request
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from query_generation import generate_sparql_query
from query_generation.generator import QueryGeneratorError

app = Flask(__name__)

@app.route('/')
def home():
    return "Hello, World!"

@app.route('/api/data', methods=['GET'])
def get_data():
    return jsonify({"message": "This is JSON data", "status": "success"})

@app.route('/api/data', methods=['POST'])
def post_data():
    data = request.get_json()
    return jsonify({"received": data, "status": "success"})

@app.route('/api/generate-sparql', methods=['POST'])
def generate_sparql():
    """
    Generate a SPARQL query from a natural language question.
    
    Request body:
    {
        "ontology": "<ontology in JSON-LD or other graph format>",
        "question": "<natural language question>"
    }
    
    Response:
    {
        "query": "<generated SPARQL query>",
        "status": "success"
    }
    """
    data = request.get_json()
    
    if not data:
        return jsonify({"error": "Request body is required", "status": "error"}), 400
    
    ontology = data.get("ontology")
    question = data.get("question")
    
    if not ontology:
        return jsonify({"error": "Ontology is required", "status": "error"}), 400
    
    if not question:
        return jsonify({"error": "Question is required", "status": "error"}), 400
    
    try:
        query = generate_sparql_query(ontology, question)
        return jsonify({"query": query, "status": "success"})
    except QueryGeneratorError as e:
        return jsonify({"error": str(e), "status": "error"}), 500
    except Exception as e:
        return jsonify({"error": f"Unexpected error: {str(e)}", "status": "error"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
