from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import case
from datetime import datetime, timedelta
import threading
import uuid
import time

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///render_system.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Define task priority constants
PRIORITY = ['RUSH', 'HIGH', 'MEDIUM', 'LOW']

# Render Task Model
class RenderTask(db.Model):
    id = db.Column(db.String(36), primary_key=True)
    status = db.Column(db.String(20), default='pending')
    priority = db.Column(db.String(20), default='MEDIUM')
    progress = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.now())
    updated_at = db.Column(db.DateTime, default=datetime.now())
    worker_id = db.Column(db.String(36), nullable=True)

ordering = case(
    {value: index for index, value in enumerate(PRIORITY)},
    value=RenderTask.priority
)

# Worker Model
class Worker(db.Model):
    id = db.Column(db.String(36), primary_key=True)
    status = db.Column(db.String(20), default='Offline')
    last_heartbeat = db.Column(db.DateTime, default=datetime.now())
    current_task_id = db.Column(db.String(36), db.ForeignKey('render_task.id'), nullable=True)

# Periodically check for worker failures and retry tasks
def check_worker_failures():
    failed_workers = Worker.query.filter(Worker.last_heartbeat < datetime.now() - timedelta(seconds=30)).all()
    print (time.ctime())
    if not failed_workers:
        worker_request_task('')
    for worker in failed_workers:
        # Task preemption or reassignment logic here
        worker_request_task(worker.id)
        pass

def worker_loop():
    with app.app_context():
        while True:
            print("Loop is running...")
            check_worker_failures()
            time.sleep(30)

# Initialize the database
def init_db():
    with app.app_context():
        db.create_all()

### 3. API Endpoints ###

# POST /api/renders - Submit a new render task
@app.route('/api/renders', methods=['POST'])
def create_render():
    data = request.json
    task_id = str(uuid.uuid4())
    priority = data.get('priority', 'MEDIUM')
    
    if priority not in PRIORITY:
        return jsonify({'error': 'Invalid priority'}), 400
    
    new_task = RenderTask(
        id=task_id,
        priority=priority
    )
    db.session.add(new_task)
    db.session.commit()
    
    return jsonify({'task_id': task_id, 'status': 'Task added to queue'}), 201


# GET /api/renders - Get status of all render tasks
@app.route('/api/renders', methods=['GET'])
def get_renders():
    tasks = RenderTask.query.all()
    task_list = [{'id': task.id, 'status': task.status, 'priority': task.priority, 'progress': task.progress} for task in tasks]
    return jsonify(task_list)


# GET /api/renders/{id} - Get detailed status of specific render
@app.route('/api/renders/<task_id>', methods=['GET'])
def get_render_status(task_id):
    task = db.session.get(RenderTask, task_id)
    if not task:
        return jsonify({'error': 'Task not found'}), 404
    
    task_info = {
        'id': task.id,
        'status': task.status,
        'priority': task.priority,
        'progress': task.progress,
        'created_at': task.created_at,
        'updated_at': task.updated_at
    }
    return jsonify(task_info)


# GET /api/workers - Get status of all render workers
@app.route('/api/workers', methods=['GET'])
def get_workers():
    workers = Worker.query.all()
    worker_list = [{'id': worker.id, 'status': worker.status, 'last_heartbeat': worker.last_heartbeat} for worker in workers]
    return jsonify(worker_list)


# POST /api/workers/{id}/request-task - Get next available task
@app.route('/api/workers/<worker_id>/request-task', methods=['POST'])
def worker_request_task(worker_id):
    noWorkers = Worker.query.filter(Worker.status == 'Ready').count()

    ## Need to check for RUSH task to preempt

    # check to see if there are rendering tasks for this worker
    renderingTask = RenderTask.query.filter(RenderTask.status == 'rendering' and RenderTask.worker_id == worker_id).first()
    if renderingTask:
        return jsonify({'message': 'Task is processing'}), 404
    
    if noWorkers == 0:
        print('Creating new worker...')
        worker_id = str(uuid.uuid4())
        new_worker = Worker(
            id = worker_id,
            status = 'Ready',
            last_heartbeat = datetime.now()
        )
        db.session.add(new_worker)
        db.session.commit()

    worker = db.session.get(Worker, worker_id)
    if not worker:
        return jsonify({'error': 'Worker not found'}), 404

    # Get the next available task based on priority
    task = RenderTask.query.filter(RenderTask.status == 'pending').order_by(ordering).first()
    if not task:
        return jsonify({'message': 'No available tasks'}), 404
    
    print("Next priority: " + task.priority)
    
    task.status = 'rendering'
    task.worker_id = worker_id
    worker.status = 'Busy'
    worker.current_task_id = task.id
    db.session.commit()

    return jsonify({'task_id': task.id, 'status': 'Task assigned to worker'})


# POST /api/workers/{id}/status - Update worker status (ready/busy/offline)
@app.route('/api/workers/<worker_id>/status', methods=['POST'])
def update_worker_status(worker_id):
    worker = db.session.get(Worker, worker_id)
    if not worker:
        return jsonify({'error': 'Worker not found'}), 404

    status = request.json.get('status')
    if status not in ['Ready', 'Busy', 'Offline']:
        return jsonify({'error': 'Invalid status'}), 400

    worker.status = status
    worker.last_heartbeat = datetime() if status != 'Offline' else None
    db.session.commit()

    return jsonify({'status': status, 'message': 'Worker status updated'})


# POST /api/renders/{id}/status - Update render task status and progress
@app.route('/api/renders/<task_id>/status', methods=['POST'])
def update_render_status(task_id):
    task = db.session.get(RenderTask, task_id)

    if not task:
        return jsonify({'error': 'Task not found'}), 404
    
    status = request.json.get('status')
    progress = request.json.get('progress')
    
    if status:
        task.status = status
    if progress is not None:
        task.progress = progress
    
    task.updated_at = datetime()
    db.session.commit()

    return jsonify({'status': task.status, 'progress': task.progress})


# POST /api/renders/{id}/complete - Mark render task as completed with result metadata
@app.route('/api/renders/<task_id>/complete', methods=['POST'])
def complete_render(task_id):
    task = db.session.get(RenderTask, task_id)
    #task = RenderTask.query.get(task_id)
    if not task:
        return jsonify({'error': 'Task not found'}), 404

    task.status = 'Completed'
    task.updated_at = datetime()
    db.session.commit()

    return jsonify({'status': 'Completed', 'message': 'Render task completed successfully'})


# Heartbeat management for worker failures (every 30 seconds)
@app.route('/api/workers/<worker_id>/heartbeat', methods=['POST'])
def worker_heartbeat(worker_id):
    worker = db.session.get(Worker, worker_id)
    #worker = Worker.query.get(worker_id)
    if not worker:
        return jsonify({'error': 'Worker not found'}), 404
    
    worker.last_heartbeat = datetime()
    db.session.commit()

    return jsonify({'status': 'Heartbeat received'})


t=threading.Thread(target=worker_loop)
t.start()
if __name__ == '__main__':
    app.run(debug=True)