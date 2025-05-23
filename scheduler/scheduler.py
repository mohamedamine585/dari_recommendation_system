import logging
from datetime import datetime, timezone
import requests

from flask import Flask, render_template, jsonify
from flask_sqlalchemy import SQLAlchemy
from apscheduler.schedulers.background import BackgroundScheduler

# Configuration Flask
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://spark_user:spark_pass@localhost/spark_scheduler'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

# DB
db = SQLAlchemy(app)

from sqlalchemy.dialects.mysql import LONGTEXT

class JobExecution(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    job_name = db.Column(db.String(255))
    start_time = db.Column(db.DateTime, default=datetime.utcnow)
    end_time = db.Column(db.DateTime)
    status = db.Column(db.String(50), default='RUNNING')
    details = db.Column(LONGTEXT)


# Envoi de la commande à http://localhost:5000/execute
def send_spark_command():
    with app.app_context():
        job = JobExecution(
            job_name="recommendation_job.py",
            status="SENT",
            start_time=datetime.now(timezone.utc),
            details="Sending spark-submit request to /execute"
        )
        db.session.add(job)
        db.session.commit()

        command = "spark-submit --packages mysql:mysql-connector-java:8.0.28 /opt/spark/recommendation_job.py"
        try:
            logger.info(f"Sending command to receiver: {command}")
            response = requests.post("http://localhost:5000/execute", json={"command": command})
            response.raise_for_status()
            result = response.json()

            job.end_time = datetime.now(timezone.utc)
            job.status = 'COMPLETED' if result.get("returncode") == 0 else 'FAILED'
            job.details = f"STDOUT:\n{result.get('stdout')}\n\nSTDERR:\n{result.get('stderr')}"
        except Exception as e:
            job.end_time = datetime.now(timezone.utc)
            job.status = 'FAILED'
            job.details = f"Exception: {str(e)}"

        db.session.commit()
        logger.info(f"Job sent and recorded with status {job.status}")

# Scheduler (toutes les 10 minutes)
scheduler = BackgroundScheduler()
scheduler.add_job(func=send_spark_command, trigger="interval", minutes=10)
scheduler.start()

# Routes
@app.route('/')
def index():
    executions = JobExecution.query.order_by(JobExecution.start_time.desc()).limit(20).all()
    return render_template("index.html", executions=executions)

@app.route('/job/<int:job_id>')
def job_details(job_id):
    execution = JobExecution.query.get_or_404(job_id)
    return render_template("job_details.html", execution=execution)

@app.route('/api/trigger', methods=['POST'])
def manual_trigger():
    send_spark_command()
    return jsonify({"status": "ok", "message": "Job manually triggered"}), 200

# Entrée principale
if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        send_spark_command()  # Envoi immédiat au démarrage
    logger.info("Démarrage de l'application Flask Spark Scheduler")
    app.run(host='0.0.0.0', port=5001)
