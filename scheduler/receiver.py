import logging
import subprocess
from flask import Flask, request, jsonify

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/execute', methods=['POST'])
def execute_command():
    try:
        data = request.get_json()
        command = data.get('command')
        if not command:
            return jsonify({"status": "error", "message": "No command provided"}), 400

        logger.info(f"Received command: {command}")
        # Version compatible avec Python < 3.7
        result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        output = {
            "status": "success",
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode
        }
        logger.info(f"Command executed: {output}")
        return jsonify(output)
    except Exception as e:
        logger.error(f"Error executing command: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting receiver Flask app")
    app.run(host='0.0.0.0', port=5000)
